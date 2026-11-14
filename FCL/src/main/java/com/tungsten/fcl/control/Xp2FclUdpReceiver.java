package com.tungsten.fcl.control;

import android.os.Handler;
import android.os.Looper;
import android.os.SystemClock;
import android.util.Log;
import com.tungsten.fclauncher.keycodes.FCLKeycodes;
import com.tungsten.fclauncher.keycodes.LwjglKeycodeMap;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** XP2FCL UDP input receiver. All FCLInput calls run on Android's main thread. */
public final class Xp2FclUdpReceiver {
    public static final int PORT = 39555;
    private static final long TIMEOUT_MS = 1500;
    private final FCLInput input;
    private final Handler main = new Handler(Looper.getMainLooper());
    private final Map<String,Integer> keys = new HashMap<>();
    private final Set<Integer> wireDown = new HashSet<>(), deliveredDown = new HashSet<>();
    private volatile DatagramSocket socket;
    private volatile Thread thread;
    private volatile int generation;
    private boolean startRequested;
    private boolean stopping;
    private String session;
    private long lastPacket, lastSeq = -1;

    public Xp2FclUdpReceiver(FCLInput input) {
        if (input == null) throw new IllegalArgumentException("input is null");
        this.input = input;
        mapKeys();
    }

    public synchronized void start() {
        startRequested = true;
        if (thread == null) {
            launchWorkerLocked();
        }
    }

    private void launchWorkerLocked() {
        if (!startRequested || thread != null) return;

        stopping = false;
        final int g = ++generation;
        Thread worker = new Thread(() -> receive(g), "XP2FCL-UDP");
        worker.setDaemon(true);
        thread = worker;
        worker.start();
    }

    public synchronized void stopAndRelease() {
        startRequested = false;
        stopping = true;
        ++generation;

        DatagramSocket s = socket;
        socket = null;
        if (s != null) s.close();

        Thread t = thread;
        if (t != null) t.interrupt();

        if (Looper.myLooper() == main.getLooper()) {
            releaseDelivered();
        } else {
            main.post(this::releaseDelivered);
        }
    }

    private void receive(int g) {
        // 每一轮接收都从干净的协议状态开始。
        // 这些字段和 wireDown 仅由 UDP 工作线程访问。
        wireDown.clear();
        session = null;
        lastSeq = -1;
        lastPacket = 0;

        DatagramSocket s = null;
        try {
            s = new DatagramSocket(null);
            s.setReuseAddress(true);
            s.bind(new InetSocketAddress(PORT));
            s.setSoTimeout(250);
            Log.i("XP2FCL", "UDP receiver bound to port " + PORT);
            synchronized (this) {
                if (generation != g || thread != Thread.currentThread()) {
                    s.close();
                    return;
                }
                socket = s;
            }

            byte[] buf = new byte[512];
            while (generation == g && !s.isClosed()) {
                try {
                    DatagramPacket p = new DatagramPacket(buf, buf.length);
                    s.receive(p);
                    Log.i("XP2FCL", "UDP received: " + new String(p.getData(), p.getOffset(), p.getLength(), StandardCharsets.US_ASCII));
                    if (p.getLength() == 0 || p.getLength() >= buf.length) continue;
                    String msg = new String(p.getData(), p.getOffset(), p.getLength(),
                            StandardCharsets.US_ASCII).trim();
                    parse(msg, g);
                } catch (SocketTimeoutException e) {
                    expire(g);
                }
            }
        } catch (IOException e) {
            if (generation == g) Log.e("XP2FCL", "UDP receiver failed", e);
        } finally {
            if (s != null) s.close();

            synchronized (this) {
                if (socket == s) socket = null;

                if (thread == Thread.currentThread()) {
                    thread = null;
                }

                // 只有停止期间又收到启动请求，才在旧线程退出后重启。
                if (stopping && startRequested) {
                    launchWorkerLocked();
                }
            }
        }
    }

    // Called only by the UDP thread.
    private void parse(String msg, int g) {
        if (generation != g) return;
        expire(g);
        if (generation != g) return;

        String[] p = msg.split("\\|", -1);
        if (p.length == 2 && p[0].equals("H")) {
            String id = p[1];
            if (!validSession(id)) return;
            if (id.equals(session)) {
                lastPacket = now();
                return;
            }
            if (session != null && fresh()) return;
            releaseWire(g);
            session = id; lastSeq = -1; lastPacket = now();
            return;
        }
        if (p.length == 6 && p[0].equals("K")) {
            long seq = sequence(p[2]);
            Integer code = keys.get(norm(p[3]));
            int ch = hexChar(p[5]);
            if (!current(p[1]) || seq <= lastSeq || code == null || ch < 0
            || !(p[4].equals("D") || p[4].equals("U"))) return;
            Log.i("XP2FCL", "Accepted key: " + p[3] + " " + p[4] + " seq=" + seq);
            lastSeq = seq; lastPacket = now();
            change(code, p[4].equals("D"), p[4].equals("D") ? ch : 0, g);
            return;
        }
        if (p.length == 4 && p[0].equals("S")) {
            long seq = sequence(p[2]);
            if (!current(p[1]) || seq <= lastSeq) return;
            Set<Integer> wanted = new HashSet<>();
            if (!p[3].equals("-") && !p[3].isEmpty()) {
                for (String name : p[3].split(",")) {
                    Integer code = keys.get(norm(name));
                    if (code == null) return;
                    wanted.add(code);
                }
            }
            lastSeq = seq; lastPacket = now();
            for (Integer k : new HashSet<>(wireDown))
                if (!wanted.contains(k)) change(k, false, 0, g);
            for (Integer k : wanted)
                if (!wireDown.contains(k)) change(k, true, 0, g);
            return;
        }
        // Legacy packets: "W DOWN" and "W UP".
        String[] old = msg.split("\\s+");
        if (old.length == 2 && session == null) {
            Integer code = keys.get(norm(old[0]));
            if (code == null) return;
            if (old[1].equalsIgnoreCase("DOWN")) {
                change(code, true, 0, g); lastPacket = now();
            } else if (old[1].equalsIgnoreCase("UP")) {
                change(code, false, 0, g); lastPacket = now();
            }
        }
    }

    private void change(int code, boolean down, int ch, int g) {
        boolean changed = down ? wireDown.add(code) : wireDown.remove(code);
        if (!changed) return;
        main.post(() -> {
            if (generation != g) return;
            if (down) {
                if (deliveredDown.add(code)) input.sendKeyEvent(code, ch, true);
            } else if (deliveredDown.remove(code)) input.sendKeyEvent(code, false);
        });
    }

    private void releaseWire(int g) {
        for (Integer code : new HashSet<>(wireDown)) change(code, false, 0, g);
    }

    private void expire(int g) {
        if (generation == g && lastPacket != 0 && now() - lastPacket > TIMEOUT_MS) {
            releaseWire(g);
            session = null; lastSeq = -1; lastPacket = 0;
        }
    }

    private boolean current(String id) {
        return session != null && session.equals(id) && validSession(id);
    }
    private boolean fresh() {
        return now() - lastPacket <= TIMEOUT_MS;
    }
    private static long now() {
        return SystemClock.elapsedRealtime();
    }
    private static long sequence(String s) {
        try {
            long n = Long.parseLong(s);
            return n >= 0 ? n : -1;
        } catch (NumberFormatException e) {
            return -1;
        }
    }
    private static int hexChar(String s) {
        if (s.length() != 4) return -1;
        try {
            return Integer.parseInt(s, 16);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
    private static boolean validSession(String s) {
        return s != null && s.matches("[0-9A-Fa-f]{8,64}");
    }
    private static String norm(String s) {
        return s.trim().toUpperCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
    }
    private void add(String s, int c) {
        if (LwjglKeycodeMap.convertKeycode(c) != 0) {
            keys.put(s, c);
        }
    }

    private void releaseDelivered() {
        for (Integer code : new HashSet<>(deliveredDown)) {
            input.sendKeyEvent(code, false);
        }
        deliveredDown.clear();
    }

    private void mapKeys() {
        int[] letters = {FCLKeycodes.KEY_A,FCLKeycodes.KEY_B,FCLKeycodes.KEY_C,FCLKeycodes.KEY_D,
            FCLKeycodes.KEY_E,FCLKeycodes.KEY_F,FCLKeycodes.KEY_G,FCLKeycodes.KEY_H,
            FCLKeycodes.KEY_I,FCLKeycodes.KEY_J,FCLKeycodes.KEY_K,FCLKeycodes.KEY_L,
            FCLKeycodes.KEY_M,FCLKeycodes.KEY_N,FCLKeycodes.KEY_O,FCLKeycodes.KEY_P,
            FCLKeycodes.KEY_Q,FCLKeycodes.KEY_R,FCLKeycodes.KEY_S,FCLKeycodes.KEY_T,
            FCLKeycodes.KEY_U,FCLKeycodes.KEY_V,FCLKeycodes.KEY_W,FCLKeycodes.KEY_X,
            FCLKeycodes.KEY_Y,FCLKeycodes.KEY_Z};
        for (int i=0;i<26;i++) add(String.valueOf((char)('A'+i)), letters[i]);
        int[] digits = {FCLKeycodes.KEY_0,FCLKeycodes.KEY_1,FCLKeycodes.KEY_2,FCLKeycodes.KEY_3,
            FCLKeycodes.KEY_4,FCLKeycodes.KEY_5,FCLKeycodes.KEY_6,FCLKeycodes.KEY_7,
            FCLKeycodes.KEY_8,FCLKeycodes.KEY_9};
        for (int i=0;i<10;i++) add(String.valueOf(i), digits[i]);
        add("ESC",FCLKeycodes.KEY_ESC); add("ESCAPE",FCLKeycodes.KEY_ESC);
        add("MINUS",FCLKeycodes.KEY_MINUS); add("EQUAL",FCLKeycodes.KEY_EQUAL);
        add("BACKSPACE",FCLKeycodes.KEY_BACKSPACE); add("TAB",FCLKeycodes.KEY_TAB);
        add("LEFT_BRACKET",FCLKeycodes.KEY_LEFTBRACE); add("RIGHT_BRACKET",FCLKeycodes.KEY_RIGHTBRACE);
        add("ENTER",FCLKeycodes.KEY_ENTER); add("SPACE",FCLKeycodes.KEY_SPACE);
        add("SEMICOLON",FCLKeycodes.KEY_SEMICOLON); add("APOSTROPHE",FCLKeycodes.KEY_APOSTROPHE);
        add("GRAVE",FCLKeycodes.KEY_GRAVE); add("BACKSLASH",FCLKeycodes.KEY_BACKSLASH);
        add("COMMA",FCLKeycodes.KEY_COMMA); add("PERIOD",FCLKeycodes.KEY_DOT);
        add("DOT",FCLKeycodes.KEY_DOT); add("SLASH",FCLKeycodes.KEY_SLASH);
        add("LEFT_SHIFT",FCLKeycodes.KEY_LEFTSHIFT); add("SHIFT",FCLKeycodes.KEY_LEFTSHIFT);
        add("RIGHT_SHIFT",FCLKeycodes.KEY_RIGHTSHIFT);
        add("LEFT_CTRL",FCLKeycodes.KEY_LEFTCTRL); add("CTRL",FCLKeycodes.KEY_LEFTCTRL);
        add("RIGHT_CTRL",FCLKeycodes.KEY_RIGHTCTRL);
        add("LEFT_ALT",FCLKeycodes.KEY_LEFTALT); add("ALT",FCLKeycodes.KEY_LEFTALT);
        add("RIGHT_ALT",FCLKeycodes.KEY_RIGHTALT);
        add("CAPS_LOCK",FCLKeycodes.KEY_CAPSLOCK); add("NUM_LOCK",FCLKeycodes.KEY_NUMLOCK);
        add("SCROLL_LOCK",FCLKeycodes.KEY_SCROLLLOCK); add("PRINT_SCREEN",FCLKeycodes.KEY_SYSRQ);
        add("SYSRQ",FCLKeycodes.KEY_SYSRQ); add("PAUSE",FCLKeycodes.KEY_PAUSE);
        add("LEFT_SUPER",FCLKeycodes.KEY_LEFTMATA); add("LEFT_META",FCLKeycodes.KEY_LEFTMATA);
        add("RIGHT_SUPER",FCLKeycodes.KEY_RIGHTMETA); add("RIGHT_META",FCLKeycodes.KEY_RIGHTMETA);
        add("UP",FCLKeycodes.KEY_UP); add("DOWN",FCLKeycodes.KEY_DOWN);
        add("LEFT",FCLKeycodes.KEY_LEFT); add("RIGHT",FCLKeycodes.KEY_RIGHT);
        add("HOME",FCLKeycodes.KEY_HOME); add("END",FCLKeycodes.KEY_END);
        add("PAGE_UP",FCLKeycodes.KEY_PAGEUP); add("PAGE_DOWN",FCLKeycodes.KEY_PAGEDOWN);
        add("INSERT",FCLKeycodes.KEY_INSERT); add("DELETE",FCLKeycodes.KEY_DELETE);
        add("KP_MULTIPLY",FCLKeycodes.KEY_KPASTERISK); add("KP_DIVIDE",FCLKeycodes.KEY_KPSLASH);
        add("KP_MINUS",FCLKeycodes.KEY_KPMINUS); add("KP_PLUS",FCLKeycodes.KEY_KPPLUS);
        add("KP_DECIMAL",FCLKeycodes.KEY_KPDOT); add("KP_ENTER",FCLKeycodes.KEY_KPENTER);
        add("KP_EQUAL",FCLKeycodes.KEY_KPEQUAL);
        int[] kp = {FCLKeycodes.KEY_KP0,FCLKeycodes.KEY_KP1,FCLKeycodes.KEY_KP2,
            FCLKeycodes.KEY_KP3,FCLKeycodes.KEY_KP4,FCLKeycodes.KEY_KP5,
            FCLKeycodes.KEY_KP6,FCLKeycodes.KEY_KP7,FCLKeycodes.KEY_KP8,FCLKeycodes.KEY_KP9};
        for (int i=0;i<10;i++) add("KP"+i,kp[i]);
        int[] fn = {FCLKeycodes.KEY_F1,FCLKeycodes.KEY_F2,FCLKeycodes.KEY_F3,FCLKeycodes.KEY_F4,
            FCLKeycodes.KEY_F5,FCLKeycodes.KEY_F6,FCLKeycodes.KEY_F7,FCLKeycodes.KEY_F8,
            FCLKeycodes.KEY_F9,FCLKeycodes.KEY_F10,FCLKeycodes.KEY_F11,FCLKeycodes.KEY_F12,
            FCLKeycodes.KEY_F13,FCLKeycodes.KEY_F14,FCLKeycodes.KEY_F15,FCLKeycodes.KEY_F16,
            FCLKeycodes.KEY_F17,FCLKeycodes.KEY_F18,FCLKeycodes.KEY_F19,FCLKeycodes.KEY_F20,
            FCLKeycodes.KEY_F21,FCLKeycodes.KEY_F22,FCLKeycodes.KEY_F23,FCLKeycodes.KEY_F24};
        for (int i=0;i<24;i++) add("F"+(i+1),fn[i]);
    }
}
