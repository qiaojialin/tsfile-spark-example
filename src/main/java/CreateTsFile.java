import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.timeseries.basis.TsFile;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfile.timeseries.write.schema.SchemaBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by qiaojialin on 2017/7/4.
 */
public class CreateTsFile {

    public static void main(String args[]) throws IOException, WriteProcessException {

        String outPath = "src/main/resources/out.tsfile";
        TSRandomAccessFileWriter fileWriter = new RandomAccessOutputStream(new File(outPath));
        SchemaBuilder builder = new SchemaBuilder();
        builder.addSeries("v", TSDataType.FLOAT, TSEncoding.RLE, null);
        builder.addSeries("a", TSDataType.FLOAT, TSEncoding.RLE, null);
        builder.addSeries("h", TSDataType.FLOAT, TSEncoding.RLE, null);
        builder.addSeries("px", TSDataType.FLOAT, TSEncoding.RLE, null);
        builder.addSeries("py", TSDataType.FLOAT, TSEncoding.RLE, null);
        builder.addSeries("status", TSDataType.INT32, TSEncoding.RLE, null);
        FileSchema schema = builder.build();
        TsFile tsFile = new TsFile(fileWriter, schema);

        String csvPath = "src/main/resources/out.csv";
        File srcFile = new File(csvPath);
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(srcFile));
            String line;
            int i = 0;
            while ((line = bufferedReader.readLine()) != null) {
                String[] items = line.split(",");
                String newLine = items[1] + "," + items[0] +
                        ",v," + items[3] +
                        ",a," + items[4] +
                        ",h," + items[5] +
                        ",px," + items[6] +
                        ",py," + items[7] +
                        ",status," + items[8];
//                if(i< 10) {
//                    i ++;
//                    System.out.println(newLine);
//                    tsFile.writeLine(newLine);
//                }
//                if(items[8].equals("0")) {
//                    System.out.println(newLine);
//                    tsFile.writeLine(newLine);
//                }
                if(items[8].equals("0"))
                    i++;
                System.out.println(newLine);
                tsFile.writeLine(newLine);
            }
            System.out.println(i);

            tsFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
