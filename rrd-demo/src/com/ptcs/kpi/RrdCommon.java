package com.ptcs.kpi;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import org.rrd4j.core.Util;
import org.rrd4j.cmd.RrdCommander;

import com.ptcs.kpi.Rrd.FetchData;
import com.ptcs.app.common.bean.ManagerBean;

public class RrdCommon {

    private static Rrd rrd;
    private static String osName;
    private static String analysisPath;
    private static String wholeDir;
    private static String wholeName;
    private static String createCmd;
    private static String updateCmd;
    private static String fetchBizCmd;
    private static String fetchHostCmd;
    private static boolean isOsLinux;

    static {
        try {
            Properties storeProp = new Properties();
            InputStream fis = RrdCommon.class.getResourceAsStream("/store.properties");
            storeProp.load(fis);
            analysisPath = storeProp.getProperty("system.analysis.path");
            Properties osProps = System.getProperties();
            osName = osProps.getProperty("os.name");
            System.out.println(osName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        rrd = Rrd.getInstance();
        wholeDir = "%OS_RRDPATH%/ipm/rrd/app/%BIZNAME%/";
        wholeName = "%OS_RRDPATH%/ipm/rrd/app/%BIZNAME%/%KPINAME%.rrd";
        createCmd = "create %OS_RRDPATH%/ipm/rrd/app/%BIZNAME%/%KPINAME%.rrd --start %TIME% --step 60 " +                      
            "DS:NORMAL:GAUGE:90:U:U DS:NORMAL_ALERT:GAUGE:90:U:U " +                                                           
            "DS:IMPORTANT_ALERT:GAUGE:90:U:U DS:URGENT_ALERT:GAUGE:90:U:U " +
            "RRA:AVERAGE:0.5:1:144000";
        updateCmd = "update %OS_RRDPATH%/ipm/rrd/app/%BIZNAME%/%KPINAME%.rrd " + 
            "%TIME%:%NORMAL%:%NORMAL_ALERT%:%IMPORTANT_ALERT%:%URGENT_ALERT%";
        fetchBizCmd= "fetch %OS_RRDPATH%/ipm/rrd/app/%BIZNAME%/%KPINAME%.rrd " + 
            "AVERAGE --start %STARTTIME% --end %ENDTIME% --resolution 60";
        fetchHostCmd = "fetch --daemon unix:/tmp/rrdcached.sock %OS_RRDPATH%/ipm/rrd/interfaces/device.2/hosts/" + 
            "%IP0%/%IP1%/%IP2%/%IP3%/%PORT%/%KPINAME%.rrd " + 
            "AVERAGE --start %STARTTIME% --end %ENDTIME% --resolution 60";

        if (osName.equals("Linux")) {
            isOsLinux = true;
            wholeDir = wholeDir.replaceFirst("%OS_RRDPATH%", analysisPath);
            wholeName = wholeName.replaceFirst("%OS_RRDPATH%", analysisPath);
            createCmd = createCmd.replaceFirst("%OS_RRDPATH%", analysisPath);
            updateCmd = updateCmd.replaceFirst("%OS_RRDPATH%", analysisPath);
            fetchBizCmd = fetchBizCmd.replaceFirst("%OS_RRDPATH%", analysisPath);
            fetchHostCmd = fetchHostCmd.replaceFirst("%OS_RRDPATH%", analysisPath);
        } else {
            isOsLinux = false;
            wholeDir = wholeDir.replaceFirst("%OS_RRDPATH%", "C:/kpi/ipm/rrd/");
            wholeName = wholeName.replaceFirst("%OS_RRDPATH%", "C:/kpi/ipm/rrd/");
            createCmd = createCmd.replaceFirst("%OS_RRDPATH%", "C:/kpi/ipm/rrd/");
            updateCmd = updateCmd.replaceFirst("%OS_RRDPATH%", "C:/kpi/ipm/rrd/");
            fetchBizCmd = fetchBizCmd.replaceFirst("%OS_RRDPATH%", "C:/kpi/ipm/rrd/");
            fetchHostCmd = fetchHostCmd.replaceFirst("%OS_RRDPATH%", "C:/kpi/ipm/rrd/");
        }
    }

    /**
     * @Title: rrdWriteBizData
     * @Description: rrd写入业务数据
     * @param @param time   -- 时间戳[s]
     * @param @param bizName-- 业务名称
     * @param @param kpiName-- KPI名称
     * @param @param normal -- 普通
     * @param @param normalAlert -- 普通告警
     * @param @param importantAlert-- 重要告警
     * @param @param urgentAlert-- 紧急告警
     */
    public static void rrdWriteBizData(long time, String bizName, String kpiName, 
            double normal, double normalAlert, double importantAlert, double urgentAlert) {
        try {
            String update = updateCmd;
            time = Util.normalize(time, 60);
            update = update
                .replaceFirst("%BIZNAME%", bizName)
                .replaceFirst("%KPINAME%", kpiName)
                .replaceFirst("%TIME%", String.valueOf(time))
                .replaceFirst("%NORMAL%", String.valueOf(normal))
                .replaceFirst("%NORMAL_ALERT%", String.valueOf(normalAlert))
                .replaceFirst("%IMPORTANT_ALERT%", String.valueOf(importantAlert))
                .replaceFirst("%URGENT_ALERT%", String.valueOf(urgentAlert));
            RrdCommon.createDir(time, bizName, kpiName);
            System.out.println(update);
            if (isOsLinux) {
                rrd.update(update);
            } else {
                RrdCommander.execute(update);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @Title: rrdFetchBiz
     * @Description: rrd获取业务数据
     * @param @param start	-- 开始时间(时间戳[s])
     * @param @param end	-- 结束时间(时间戳[s])
     * @param @param bizName-- 业务名称
     * @param @param kpiName-- KPI名称
     * @return 返回数据
     */
    public static List<RrdBizBean> rrdFetchBiz(long start, long end, String bizName, String kpiName) {
        String fetch = fetchBizCmd;
        List<RrdBizBean> fetchList = new ArrayList<RrdBizBean>();
        try {
            String[] setime = calcStartEndTime(start, end).split(",");
            fetch = fetch
                .replaceFirst("%BIZNAME%", bizName)
                .replaceFirst("%KPINAME%", kpiName)
                .replaceFirst("%STARTTIME%", setime[0])
                .replaceFirst("%ENDTIME%", setime[1]);
            System.out.println(fetch);
            if (isOsLinux) {
                com.ptcs.kpi.Rrd.FetchData data = (com.ptcs.kpi.Rrd.FetchData )rrd.fetch(fetch);
                int rows = data.getRowCount();
                long time = 0l;
                if (rows > 0) {
                    for (int i = 0; i < rows; i++) {
                        time  = data.getTimestamp(i) + 60;
                        if (start >= time) {continue;}
                        if (end < time) {break;}
                        RrdBizBean rbb = new RrdBizBean();
                        rbb.setNormal((Double.isNaN(data.getValue(i, 0)) ? 0.0 : data.getValue(i, 0)));
                        rbb.setNormalAlert((Double.isNaN(data.getValue(i, 1)) ? 0.0 : data.getValue(i, 1)));
                        rbb.setImportantAlert((Double.isNaN(data.getValue(i, 2)) ? 0.0 : data.getValue(i, 2)));
                        rbb.setUrgentAlert((Double.isNaN(data.getValue(i, 3)) ? 0.0 : data.getValue(i, 3)));
                        fetchList.add(rbb);
                    }
                }
            } else {
                org.rrd4j.core.FetchData data = (org.rrd4j.core.FetchData )RrdCommander.execute(fetch);
                int rows = data.getRowCount();
                long time = 0l;
                if (rows > 0) {
                    for (int i = 0; i < rows; i++) {
                        time  = data.getTimestamps()[i] + 60;
                        if (start >= time) {continue;}
                        if (end < time) {break;}
                        RrdBizBean rbb = new RrdBizBean();
                        rbb.setNormal((Double.isNaN(data.getValues(0)[i]) ? 0.0 : data.getValues(0)[i]));
                        rbb.setNormalAlert((Double.isNaN(data.getValues(1)[i]) ? 0.0 : data.getValues(1)[i]));
                        rbb.setImportantAlert((Double.isNaN(data.getValues(2)[i]) ? 0.0 : data.getValues(2)[i]));
                        rbb.setUrgentAlert((Double.isNaN(data.getValues(3)[i]) ? 0.0 : data.getValues(3)[i]));
                        fetchList.add(rbb);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fetchList;
    }

    /**
     * @Title: rrdFetchHost
     * @Description: rrd获取主机数据
     * @param @param start	-- 开始时间(时间戳[s])
     * @param @param end	-- 结束时间(时间戳[s])
     * @param @param mb     -- 业务系统对象
     * @param @param kpiName-- KPI名称
     * @return 返回数据
     */
    public static List<Double> rrdFetchHost(long start, long end, ManagerBean mb, String kpiName) {
        String fetch = fetchHostCmd;
        List<Double> fetchList = new ArrayList<Double>();
        try {
            String[] ips = mb.getIp().split(".");
            String[] setime = calcStartEndTime(start, end).split(",");
            fetch = fetch
                .replaceFirst("%IP0%", ips[0])
                .replaceFirst("%IP1%", ips[1])
                .replaceFirst("%IP2%", ips[2])
                .replaceFirst("%IP3%", ips[3])
                .replaceFirst("%PORT%", mb.getPort())
                .replaceFirst("%KPINAME%", kpiName)
                .replaceFirst("%STARTTIME%", setime[0])
                .replaceFirst("%ENDTIME%", setime[1]);
            if (isOsLinux) {
                com.ptcs.kpi.Rrd.FetchData data = (com.ptcs.kpi.Rrd.FetchData )rrd.fetch(fetch);
                int rows = data.getRowCount();
                long time = 0l;
                if (rows > 0) {
                    for (int i = 0; i < rows; i++) {
                        time  = data.getTimestamp(i) + 60;
                        if (start >= time) {continue;}
                        if (end < time) {break;}
                        fetchList.add((Double.isNaN(data.getValue(i, 0)) ? 0.0 : data.getValue(i, 0)));
                    }
                }
            } else {
                org.rrd4j.core.FetchData data = (org.rrd4j.core.FetchData )RrdCommander.execute(fetch);
                int rows = data.getRowCount();
                long time = 0l;
                if (rows > 0) {
                    for (int i = 0; i < rows; i++) {
                        time  = data.getTimestamps()[i] + 60;
                        if (start >= time) {continue;}
                        if (end < time) {break;}
                        fetchList.add((Double.isNaN(data.getValues(0)[i]) ? 0.0 : data.getValues(0)[i]));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fetchList;
    }

    /**
     * @Title: rrdFetchHostList
     * @Description: rrd获取多台主机平均数据
     * @param @param start	-- 开始时间(时间戳[s])
     * @param @param end	-- 结束时间(时间戳[s])
     * @param @param mbList -- 业务系统对象列表
     * @param @param kpiName-- KPI名称
     * @return 返回数据
     */
    public static List<Double> rrdFetchHostList(long start, long end, List<ManagerBean> mbList, String kpiName) {
        List<Double> valList = null;
        List<Double> retList = new ArrayList<Double>();
        for (ManagerBean mb: mbList) {
            valList = RrdCommon.rrdFetchHost(start, end, mb, kpiName);
            for (int i = 0;i < valList.size();i++) {
                retList.set(i, Double.valueOf(retList.get(i).doubleValue() + valList.get(i).doubleValue()));
            }
        }
        for (int i = 0;i < retList.size();i++) {
            retList.set(i, Double.valueOf(retList.get(i).doubleValue() / mbList.size()));
        }
        return retList;
    }

    private static void createDir(long time, String bizName, String kpiName) throws Exception {
        String create = createCmd, whole = wholeDir;
        whole = whole.replaceFirst("%BIZNAME%", bizName);
        File file = new File(whole);
        file.mkdirs();
        whole = wholeName;
        whole = whole.replaceFirst("%BIZNAME%", bizName).replaceFirst("%KPINAME%", kpiName);
        file = new File(whole);
        if (!file.exists()) {
            create = create
                .replace("%BIZNAME%", bizName)
                .replace("%KPINAME%", kpiName)
                .replace("%TIME%", String.valueOf(time-1));
            System.out.println(create);
            if (isOsLinux) {
                rrd.create(create);
            } else {
                RrdCommander.execute(create);
            }
        }
    }

    private static String calcStartEndTime(long startTime, long endTime) {
        String startstr = null, endsrt = null;
        long start = startTime, end = endTime, mod = 0;;
        long nowTime = System.currentTimeMillis() / 1000;

        if (start != 0) {
            long m = nowTime - start; 
            long n = nowTime - end;   
            mod = m % 10;
            m -= mod;
            mod = n % 10;
            n -= mod;
            if (n < 30) {
                startstr = "-" + (m + 30) + "s";
                endsrt = "-" + (n + 30) + "s";
            } else {                                                                                                                                     
                startstr = "-" + m + "s"; 
                endsrt = "-" + n + "s";   
            }        
        } else {     
            startstr = "-330s";       
            endsrt = "-30s";          
        }
        return startstr + "," + endsrt;
    }

    public static void main(String[] args) {
        while (true) {
            try {
                long now = System.currentTimeMillis() / 1000;                                                                      
                RrdCommon.rrdWriteBizData(now, "a", "test", 1.4f, 2.5f, 3.6f, 4.7f);                                               
                List<RrdBizBean> data = RrdCommon.rrdFetchBiz(now - 600, now, "a", "test");
                System.out.println(data);
                Thread.sleep(60000);
            } catch (Exception e) {
                e.printStackTrace(); 
            }
        }
    }
}
