package com.maxplus1.hd_client.hbase.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.security.UserGroupInformation;
import sun.security.krb5.KrbException;

import java.io.*;
import java.net.InetAddress;

/**
 * Created by qxloo on 2017/1/8.
 */
@Slf4j
public class FusionInsightConfFactory {
    private static final String USERNAME_CLIENT_KEYTAB_FILE = "username.client.keytab.file";
    private static final String USERNAME_CLIENT_KERBEROS_PRINCIPAL = "username.client.kerberos.principal";

    private static String confPath;
    private static String keytabFile;
    private static String kerberosPrincipal;

    private FusionInsightConfFactory(String confPath, String keytabFile, String kerberosPrincipal) throws IOException {
        FusionInsightConfFactory.confPath = confPath;
        FusionInsightConfFactory.keytabFile = keytabFile;
        FusionInsightConfFactory.kerberosPrincipal = kerberosPrincipal;
    }

    private FusionInsightConfFactory(){

    }

    private static Configuration buildConfiguration() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        String hbaseDir = FusionInsightConfFactory.confPath;
        File file = new File(hbaseDir);
        if (file.isDirectory()) {
            File[] hbaseConfFiles = file.listFiles();
            for (File tmp : hbaseConfFiles) {
                if (tmp.getName().endsWith("xml")) {
                    try {
                        FileInputStream fis = new FileInputStream(tmp);
                        conf.addResource(fis);
                    } catch (FileNotFoundException e) {
                        log.warn("[WARN===>>>FileNotFoundException]",e);
                    }
                }
            }
        }

        String blufilepath = FusionInsightConfFactory.confPath;
        modifyJaas(blufilepath);

        System.setProperty("zookeeper.server.principal", "zookeeper/hadoop.hadoop.com");
        // jaas.conf file, it is included in the client pakcage file
        System.setProperty("java.security.auth.login.config", blufilepath + "jaas.conf");
        conf.set("operations.security.authentication", "kerberos");
        conf.set(USERNAME_CLIENT_KERBEROS_PRINCIPAL, FusionInsightConfFactory.kerberosPrincipal);
        conf.set(USERNAME_CLIENT_KEYTAB_FILE, blufilepath + FusionInsightConfFactory.keytabFile);
        // set the kerberos server info,point to the kerberosclient
        // configuration file.
        System.setProperty("java.security.krb5.conf", blufilepath + "krb5.conf");

        javax.security.auth.login.Configuration.getConfiguration().refresh();
        try {
            sun.security.krb5.Config.refresh();
        } catch (KrbException e) {
            log.error(" refresh krb5.conf: " + blufilepath + "krb5.conf, failed", e);
            throw new IllegalArgumentException(e);
        }
        loginKerberos(conf);
        return conf;
    }


    /**
     * 登录
     *
     * @param conf
     */
    private static void loginKerberos(Configuration conf) {
        try {
            UserGroupInformation.setConfiguration(conf);
            User.login(conf, USERNAME_CLIENT_KEYTAB_FILE, USERNAME_CLIENT_KERBEROS_PRINCIPAL, InetAddress.getLocalHost().getCanonicalHostName());
            ZKUtil.loginClient(conf, USERNAME_CLIENT_KEYTAB_FILE, USERNAME_CLIENT_KERBEROS_PRINCIPAL, InetAddress.getLocalHost().getCanonicalHostName());
        } catch (Exception e) {
            log.error("Failed to connect the operations.", e);
        }
    }


    /**
     * 将jaas文件中的keytab文件路径改为绝对路径
     * @param path
     */
    private static void modifyJaas(String path) throws IOException {
        File jaas = new File(path, "jaas.conf");
        File temp = new File(path, "jaas.conf" + "temp");
        String s;
        FileReader fr=null;
        BufferedReader in=null;
        FileWriter fw=null;
        PrintWriter pw=null;
        try {
            fr = new FileReader(jaas);//源文件
            in = new BufferedReader(fr);
            fw = new FileWriter(temp);//缓存文件
            pw = new PrintWriter(fw);
            while ((s = in.readLine()) != null) {
                if (s.contains("keyTab")) {
                    String p[] = s.split("=\"");
                    p[1] = path.concat("user.keytab\"");
                    s = p[0] + "=\"" + p[1];
                    pw.println(s);
                    pw.flush();
                } else {
                    pw.println(s);
                    pw.flush();
                }
            }
            jaas.delete();
            temp.renameTo(jaas);
        } catch (Exception e) {
            log.error("Failed to modify the jaas file.", e);
        }finally {
            if(pw!=null) pw.close();
            if(fw!=null) fw.close();
            if(in!=null) in.close();
            if(fr!=null) fr.close();
        }
    }
}
