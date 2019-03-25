
package qcc.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;


public class MyHdfsClientDemo {
    private FileSystem fs = null;
    private Configuration conf = null;

    public void init() throws Exception {
        conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://mini1:9000");
        fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
        //fs = FileSystem.get(conf);
    }


    public void upLoad() throws Exception {
        FSDataOutputStream outputStream = fs.create(new Path("/myUploadTest.txt.bak"));
        FileInputStream fileInputStream = new FileInputStream("d:/myUploadTest.txt");
        IOUtils.copy(fileInputStream, outputStream);
    }


    public void upLoad1() throws Exception {
        fs.copyFromLocalFile(new Path("d:/myUploadTest.txt"), new Path("/myUploadTest.txt.bak"));
    }

    public void delete(String path) throws IOException {
        if (fs.exists(new Path(path)))
            fs.delete(new Path(path), true);
    }

    public void mkdirs(String path) throws IOException {
        boolean mkdirs = fs.mkdirs(new Path(path));
    }

    public void downLoad(String path, String dst) throws IOException {
        FSDataInputStream fsDataInputStream = fs.open(new Path(path));
        FileOutputStream fileOutputStream = new FileOutputStream(dst);

        IOUtils.copy(fsDataInputStream, fileOutputStream);
    }

    public void listContent(String path) throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(path), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            System.out.println(next);
        }
    }

    public void fileStatus(String path) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        for (FileStatus file : fileStatuses) {
            System.out.println(file.toString());
        }
    }

    private void listConf() {
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> stringEntry = iterator.next();
            System.out.println(stringEntry.getKey()+stringEntry.getValue());
        }
    }

    public static void main(String[] args) throws Exception {
        MyHdfsClientDemo demo = new MyHdfsClientDemo();
        demo.init();
        demo.delete("/myUploadTest.txt.bak");
        demo.listContent("/");
        demo.listConf();
    }

}
