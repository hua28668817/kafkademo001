package com.rhzz.nbp.kafka01.common;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CsvUtil {

    public static void main(String args[]){
        //初始化路径
        String JarPath = CsvUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String CurrentPath = JarPath.substring(0,JarPath.lastIndexOf("/"));
        String SrcPath = CurrentPath + "/../";
        String sTitle0 = "tbid,label";
        String sTitleother = "tbid,userid";
        createCSV(sTitle0, SrcPath, "tb0.csv");
        createCSV(sTitleother, SrcPath, "tb1.csv");
        createCSV(sTitleother, SrcPath, "tb2.csv");
        createCSV(sTitleother, SrcPath, "tb3.csv");

        List listsource = new ArrayList();
        listsource.add("1,tomcat");
        listsource.add("2,Jboss");
        listsource.add("1,tomcat");
        listsource.add("2,Jboss");
        listsource.add("1,tomcat");
        listsource.add("2,Jboss");
        listsource.add("1,tomcat");
        listsource.add("2,Jboss");
        // 按行写入
        writeCSVLine("1","1center","tb0.csv");
        //一次性写入
        writeCSVAll(listsource,"tb1.csv");
        //读取整个文件
        readCSV("tb1.csv");

    }




    //创建CSV文件
    public static void createCSV(String sTitle, String Path, String fName)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FileOutputStream fos = null;
        try
        {
            out.write(sTitle.getBytes());
            out.write(",".getBytes());
            out.write("\n".getBytes());
            File newfile = new File(Path,fName);
            fos = new FileOutputStream(newfile);
            fos.write(out.toByteArray());
            fos.flush();
            out.close();
            fos.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }finally{
            try
            {
                if(null != out) out.close();
                if(null != fos) fos.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        System.out.println("生成"+fName+"完成");
    }

    //向CSV文件追加一行数据
    public static void writeCSVLine(String string1,String string2,String fName)
    {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(fName, true)); // 附加
            // 添加新的数据行
            bw.write(string1 + "," + string2);
            bw.newLine();
            bw.close();
        } catch (FileNotFoundException e) {
            // File对象的创建过程中的异常捕获
            e.printStackTrace();
        } catch (IOException e) {
            // BufferedWriter在关闭对象捕捉异常
            e.printStackTrace();
        }
    }

    //一次性写入
    public static void writeCSVAll(List ls, String fName)
    {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(fName, true)); // 附加
            for(int i=0; i<ls.size();i++){
                bw.write((String)ls.get(i));
                bw.newLine();
            }
            bw.close();

    } catch (FileNotFoundException e) {
        // File对象的创建过程中的异常捕获
        e.printStackTrace();
    } catch (IOException e) {
        // BufferedWriter在关闭对象捕捉异常
        e.printStackTrace();
    }
}

    //读取某个CSV文件的数据
    public static void readCSV(String fName)
    {
        InputStreamReader fr = null;
        BufferedReader br = null;
        try {
            fr = new InputStreamReader(new FileInputStream(fName));
            br = new BufferedReader(fr);
            String rec = null;
            String[] argsArr = null;
            while ((rec = br.readLine()) != null) {
                argsArr = rec.split(",");
                for (int i = 0; i < argsArr.length; i++) {
                    System.out.print(argsArr[i]+"\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fr != null)
                    fr.close();
                if (br != null)
                    br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

    }
}