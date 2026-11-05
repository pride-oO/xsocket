package io.github.prideoo.xsocket;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class Pem {

    private static final Pattern BLOCK_PATTERN = Pattern.compile("-----BEGIN ([A-Z0-9 ]+)-----([^-]*)-----END \\1-----");
    private static final byte[] RSA_ALGORITHM = {0x30, 0x0D, 0x06, 0x09, 0x2A, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xF7, 0x0D, 0x01, 0x01, 0x01, 0x05, 0x00};
    private static final byte[] EC_OID = {0x06, 0x07, 0x2A, (byte) 0x86, 0x48, (byte) 0xCE, 0x3D, 0x02, 0x01};
    private static final String[] KEY_ALGORITHM_LIST = {"RSA", "EC", "Ed25519", "Ed448", "EdDSA", "RSASSA-PSS", "DSA"};

    private Pem() {
    }

    static X509ExtendedKeyManager loadKeyManager(String certPath, String keyPath) throws Exception {
        String certText = new String(Files.readAllBytes(Paths.get(certPath)), StandardCharsets.US_ASCII);
        String keyText = keyPath != null && !keyPath.isEmpty() ? new String(Files.readAllBytes(Paths.get(keyPath)), StandardCharsets.US_ASCII) : certText;
        List<Certificate> certList = new ArrayList<>();
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        PrivateKey privateKey = null;
        Matcher certMatcher = BLOCK_PATTERN.matcher(certText);
        while (certMatcher.find()) {
            if (certMatcher.group(1).equals("CERTIFICATE")) {
                certList.add(certificateFactory.generateCertificate(new ByteArrayInputStream(decode(certMatcher.group(2)))));
            }
        }
        Matcher keyMatcher = BLOCK_PATTERN.matcher(keyText);
        while (keyMatcher.find() && privateKey == null) {
            String blockType = keyMatcher.group(1);
            byte[] keyBytes = decode(keyMatcher.group(2));
            if (blockType.equals("PRIVATE KEY")) {
                privateKey = parsePkcs8(keyBytes);
            } else if (blockType.equals("RSA PRIVATE KEY")) {
                privateKey = parsePkcs8(der(0x30, concat(new byte[]{0x02, 0x01, 0x00}, RSA_ALGORITHM, der(0x04, keyBytes))));
            } else if (blockType.equals("EC PRIVATE KEY")) {
                byte[] algorithm = der(0x30, concat(EC_OID, getEcCurveOID(keyBytes)));
                privateKey = parsePkcs8(der(0x30, concat(new byte[]{0x02, 0x01, 0x00}, algorithm, der(0x04, keyBytes))));
            }
        }
        if (certList.isEmpty()) {
            throw new XSocketException("Invalid cert");
        }
        if (privateKey == null) {
            throw new XSocketException("Invalid key");
        }
        char[] password = new char[0];
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("xsocket", privateKey, password, certList.toArray(new Certificate[0]));
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, password);
        for (KeyManager keyManager : keyManagerFactory.getKeyManagers()) {
            if (keyManager instanceof X509ExtendedKeyManager) {
                return (X509ExtendedKeyManager) keyManager;
            }
        }
        throw new XSocketException("Invalid key");
    }

    private static byte[] decode(String base64) {
        return Base64.getMimeDecoder().decode(base64.trim());
    }

    private static PrivateKey parsePkcs8(byte[] keyBytes) {
        for (String algorithm : KEY_ALGORITHM_LIST) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(keyBytes));
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    private static byte[] getEcCurveOID(byte[] sec1) {
        int[] position = {0};
        readHeader(sec1, position);
        while (position[0] < sec1.length) {
            int start = position[0];
            int tag = sec1[position[0]] & 0xFF;
            int length = readHeader(sec1, position);
            if (tag == 0xA0) {
                int contentStart = position[0];
                byte[] oid = new byte[length];
                System.arraycopy(sec1, contentStart, oid, 0, length);
                return oid;
            }
            position[0] += length;
            if (position[0] <= start) {
                break;
            }
        }
        throw new XSocketException("Invalid key");
    }

    private static int readHeader(byte[] bytes, int[] position) {
        position[0]++;
        int length = bytes[position[0]++] & 0xFF;
        if ((length & 0x80) != 0) {
            int count = length & 0x7F;
            length = 0;
            for (int i = 0; i < count; i++) {
                length = (length << 8) | (bytes[position[0]++] & 0xFF);
            }
        }
        return length;
    }

    private static byte[] der(int tag, byte[] content) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(tag);
        int length = content.length;
        if (length < 0x80) {
            output.write(length);
        } else {
            int count = 0;
            for (int value = length; value > 0; value >>= 8) {
                count++;
            }
            output.write(0x80 | count);
            for (int i = count - 1; i >= 0; i--) {
                output.write((length >> (8 * i)) & 0xFF);
            }
        }
        output.write(content, 0, content.length);
        return output.toByteArray();
    }

    private static byte[] concat(byte[]... partList) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] part : partList) {
            output.write(part, 0, part.length);
        }
        return output.toByteArray();
    }

    static final class ReloadableKeyManager extends X509ExtendedKeyManager {

        private volatile X509ExtendedKeyManager keyManager;

        ReloadableKeyManager(X509ExtendedKeyManager keyManager) {
            this.keyManager = keyManager;
        }

        void setKeyManager(X509ExtendedKeyManager keyManager) {
            this.keyManager = keyManager;
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return keyManager.getClientAliases(keyType, issuers);
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            return keyManager.chooseClientAlias(keyType, issuers, socket);
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            return keyManager.getServerAliases(keyType, issuers);
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            return keyManager.chooseServerAlias(keyType, issuers, socket);
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            return keyManager.getCertificateChain(alias);
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return keyManager.getPrivateKey(alias);
        }

        @Override
        public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
            return keyManager.chooseEngineClientAlias(keyType, issuers, engine);
        }

        @Override
        public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
            return keyManager.chooseEngineServerAlias(keyType, issuers, engine);
        }
    }
}
