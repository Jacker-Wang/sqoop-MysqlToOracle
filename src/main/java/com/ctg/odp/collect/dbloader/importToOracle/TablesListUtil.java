package com.ctg.odp.collect.dbloader.importToOracle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TablesListUtil {

    public static List<String> getTables(String path) {

        List<String> tables = new ArrayList<String>();

        File file = new File(path);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                tables.add(tempString.trim());
                System.out.println("line " + line + ": " + tempString);
                line++;
            }
            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    return null;
                }
            }
            return tables;
        }
    }

}
