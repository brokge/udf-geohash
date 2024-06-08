package com.tockdata;

import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author chenlw
 * @date 2023/8/11 16:28
 */
@Description(
        name = "ToPinYinUDF",
        value = "_FUNC_(String input, String caseType, String separate,Boolean sorted) - Returns String pinyin",
        extended = "Example:\n  > SELECT _FUNC_(\'\"张三\",\"UPPERCASE\",\"#\",true\');"
)
public class ToPinYinUDF extends UDF {
    public String evaluate(String input, String caseType, String separate, Integer sorted) {
        if (input.matches(".*[\\u4E00-\\u9FA5].*")) {
            return getCompareNameByCN(input, caseType, separate, sorted == 1);
        } else {
            return getCompareStrByEN(input, caseType, separate, sorted == 1);
        }
    }

    String getCompareStrByEN(String input, String caseType, String separate, Boolean sorted) {
        String[] textArr = null;
        if (HanyuPinyinCaseType.UPPERCASE.getName().equals(caseType)) {
            textArr = input.toUpperCase().split("[^A-Za-z\\u4E00-\\u9FA5]");
        } else {
            textArr = input.toLowerCase().split("[^A-Za-z\\u4E00-\\u9FA5]");
        }
        if (sorted) {
            return Arrays.stream(textArr).sorted().collect(Collectors.joining(separate));
        } else {
            return String.join(separate, textArr);
        }
    }

    String getCompareNameByCN(String input, String caseType, String separate, Boolean sorted) {
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        if (HanyuPinyinCaseType.UPPERCASE.getName().equals(caseType)) {
            format.setCaseType(HanyuPinyinCaseType.UPPERCASE);//设置为小写
        }
        //if (HanyuPinyinToneType.WITHOUT_TONE.getName().equals(toneType)) {
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        //}
        String result = "";
        try {
            if (sorted) {
                char[] charArr = input.toCharArray();
                List<String> list = new ArrayList<>(charArr.length);
                for (int i = 0; i < charArr.length; i++) {
                    //判断是否为汉字字符
                    if (isCNChar(charArr[i])) {//"[\\u4E00-\\u9FA5]+"
                        //同字多音取第一个
                        String[] pyArr = PinyinHelper.toHanyuPinyinStringArray(charArr[i], format);
                        if (pyArr != null && pyArr.length > 0) {
                            list.add(pyArr[0]);
                        }
                        // throw new IllegalArgumentException("拼音格式化转换异常=》"+charArr[i]+"找不到对应的拼音");
                    } else {
                        if (charArr[i] > 'a' && charArr[i] < 'z') {
                            list.add(String.valueOf((char) (charArr[i] - 32)));
                        } else if (charArr[i] > 'A' && charArr[i] < 'Z') {
                            list.add(String.valueOf(charArr[i]));
                        }
                    }
                }
                result = list.stream().sorted().collect(Collectors.joining(separate));
            } else {
                result = PinyinHelper.toHanYuPinyinString(input, format, separate, true);
            }
        } catch (BadHanyuPinyinOutputFormatCombination e) {
            //e.printStackTrace();
            result = "Error";
        }
        return result;

    }

    public boolean isCNChar(char c) {
        return c >= 0x4E00 && c <= 0x9FA5;//[\u4E00-\u9FA5]
    }

    public static void main(String[] args) {
        String[] inputs = new String[10];
        inputs[0] = "数码宝贝";//
        inputs[1] = "饕餮";
        inputs[2] = "机械暴龙兽";
        inputs[3] = "战斗暴龙兽";
        inputs[4] = "省事";
        inputs[5] = "省悟";
        inputs[6] = "差不多";
        inputs[7] = "差旅";
        inputs[8] = "重点";
        inputs[9] = "重启";
        inputs[9] = "JACK WANG APPLE";
        ToPinYinUDF pingyingUdf = new ToPinYinUDF();

        for (int i = 0; i < inputs.length; i++) {
            String result = pingyingUdf.evaluate(inputs[i], "UPPERCASE", " ", 1);
            String result2 = pingyingUdf.evaluate(inputs[i], "LOWERCASE", " ", 0);
            System.out.println("input + result = " + inputs[i] + "/" + result + ";" + result2);
        }

    }
}
