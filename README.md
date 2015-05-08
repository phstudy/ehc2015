# EHC 2015

競賽資訊：http://ehc.etusolution.com/

## 初賽：商品銷售排序計算

### 資料格式

```
203.145.207.188 - - [01/Feb/2015:00:00:00 +0800] "GET /action?;act=view;uid=;pid=0005158462;cat=J,J_007,J_007_001,J_007_001_001;erUid=41ee27d6-5f83-b982-69f9-f378dc9fc11b; HTTP/1.1" 302 160 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0"
1.171.24.21 - - [01/Feb/2015:00:00:00 +0800] "GET /action?;act=view;uid=;pid=0022007845;cat=I,I_001,I_001_003,I_001_003_016;erUid=a6ef6b96-4e4-21af-e645-b582a3333d57; HTTP/1.1" 302 160 "-" "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)"
1.172.0.185 - - [01/Feb/2015:00:00:00 +0800] "GET /action?;act=view;uid=;pid=0006604986;cat=G_001_006_002;erUid=6c27cc0b-d35a-addb-feb2-561b6bb28a5; HTTP/1.1" 302 160 "-" "Mozilla/5.0 (Linux; U; Android 4.1.1; zh-tw; PadFone 2 Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30"
101.13.126.139 - - [01/Feb/2015:00:00:00 +0800] "GET /action?;act=cart;uid=;plist=0022356972,3,139,0009755432,1,199;erUid=dfba63c-23c2-f0d3-2b49-996125c66535; HTTP/1.1" 302 160 "-" "Mozilla/5.0 (iPad; CPU OS 5_0_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A405 Safari/7534.48.3"
61.58.145.131 - - [01/Feb/2015:00:00:00 +0800] "GET /action?;act=view;uid=;pid=0024134891;cat=L,L_006,L_006_002,L_006_002_030;erUid=280f45c-dccf-fb70-a3a7-1f8f4ee7661a; HTTP/1.1" 302 160 "-" "Mozilla/5.0 (Linux; U; Android 4.1.2; zh-tw; GT-P3100 Build/JZO54K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30"
114.41.4.218 - - [01/Feb/2015:00:00:01 +0800] "GET /action?;act=order;uid=U312622727;plist=0006944501,1,1069;erUid=252b97f1-25bd-39ea-6006-3f3ebf52c80; HTTP/1.1" 302 160 "-" "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0; MAARJS)"
114.43.89.52 - - [01/Feb/2015:00:00:01 +0800] "GET /action?;act=view;uid=;pid=0022226912;cat=D,D_004,D_004_028,D_004_028_095;erUid=7ec350c-3f5d-7e83-f03b-da5e92c148db; HTTP/1.1" 302 160 "-" "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)"
114.34.254.21 - - [01/Feb/2015:00:00:01 +0800] "GET /action?;act=view;uid=;pid=0023531314;cat=B,B_014,B_014_063,B_014_063_003;erUid=1f56717c-2be-6b6a-554c-b1c93634103a; HTTP/1.1" 302 160 "-" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; GTB7.5; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; eSobiSubscriber 2.0.4.16)"
```

計算的資料是一份 Web Log，主要目標是計算 `action=order` 部分的購買資訊。以下面資料為例：

```
114.41.4.218 - - [01/Feb/2015:00:00:01 +0800] "GET /action?;act=order;uid=U312622727;plist=0006944501,1,1069;erUid=252b97f1-25bd-39ea-6006-3f3ebf52c80; HTTP/1.1" 302 160 "-" "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0; MAARJS)"
```

取出購買清單 `plist`

```
plist=0006944501,1,1069
```

每組商品含有 3 筆紀組分別為：

```
plist=商品ID,數量,價格
```

多種商品的購買就接在 plist 後面：

```
plist=商品1#ID,數量,價格,商品2#ID,數量,價格[,商品N#ID,數量,價格]
```

### 專案與程式位置

競賽用的 MapReduce 程式碼在子目錄 `hadoop-itemcount/mapreduce`，它是個 gradle project，可以使用 `gradle build` 編譯，也能使用 `gradle dist` 打包成大會要求的規格。整個專案可以看到各種的工具執行方法的「試寫」，聽完初賽說明會後決定以 Hadoop MapReduce Framework 為主。

[org.phstudy.ItemCount](https://github.com/phstudy/ehc2015/blob/8c5d38a663565692c615028a702ab13599550e34/hadoop-itemcount/mapreduce/src/main/java/org/phstudy/ItemCount.java) 是 Main Class，它會開 2 個 Thread：

1. [org.phstudy.job.ComputeResult](https://github.com/phstudy/ehc2015/blob/8c5d38a663565692c615028a702ab13599550e34/hadoop-itemcount/mapreduce/src/main/java/org/phstudy/job/ComputeResult.java) 為計算 Web Logs 的 Thread
1. [org.phstudy.job.CopyFileDirect](https://github.com/phstudy/ehc2015/blob/8c5d38a663565692c615028a702ab13599550e34/hadoop-itemcount/mapreduce/src/main/java/org/phstudy/job/CopyFileDirect.java) 為上傳 Web Logs 的 Thread

### 實作心得

#### Web Logs 上傳 

上傳檔案是最先完成的部分，透過 `GZIPInputStream` 能直接讀取 `EHC_1st.tar.gz` 的內容。剛寫完時，能上傳但檔案長度比原檔略長，突然想到它實際上是個 TAR File Format，查了 Wiki 它有 512 Bytes 的 Header，簡單地將它 skip 檔案就比較接近原檔的大小，但仍不等於原檔的大小。查了 File Format 文件，它在每個資料區域都會放 `0` byte。所以當讀檔接近尾聲時，我們看到 `0` byte 的內容就停止寫入 HDFS。

#### Mapper/Reducer 實作

這邊沒有太特別的內容，就一般的 Mapper/Reducer 寫法。我們提交程式選用 [org.phstudy.EHCWebLogsMapper](https://github.com/phstudy/ehc2015/blob/8c5d38a663565692c615028a702ab13599550e34/hadoop-itemcount/mapreduce/src/main/java/org/phstudy/EHCWebLogsMapper.java) 實作的版本。另外，有試著動態一些需要重複計算用的 [Pid 與 Price 進行快取的版本](https://github.com/phstudy/ehc2015/blob/8c5d38a663565692c615028a702ab13599550e34/hadoop-itemcount/mapreduce/src/main/java/org/phstudy/EHCWebLogsCachedPidPricedMapper.java)，似乎於由於 method call 次數增加了反而拖慢了速度。

唯一算得上有優化的部分，應該只有讓切割好的 plist `String[]` 是以購買數量來移動的，往前就是 pid，往後就是 price，蠻好算的，也讓 for-loop 的次數減少一點 (非常微小就是了)。

```java
for (int x = 1; x < data.length; x += 3) {
    try {

        context.write(new Text(data[x - 1]),
                new LongWritable(Integer.parseInt(data[x]) * Integer.parseInt(data[x + 1])));

    } catch (Exception e) {
        e.printStackTrace();
    }

}
```

#### 最佳化思考

雖然同時開 2 個 Thread 會比依序執行來得快一點，但仍然很花時間的。有幾個可能的方法：

1. 思考怎麼加速上傳到 HDFS 比較快時，異想天開地喃喃自語：「不知道有沒有 api 能直接在要求 namenode 建出空的檔，存在指定的路徑與需要的大小」(這是在作夢唄!?)
1. 捨去計算的部分，只做上傳至 HDFS，等 HDFS 上傳完直接吐預先計算的結果。（但是有踩線的疑慮，所以沒有選擇這條路）
1. 由於 Mapper/Reducer 的工作其實沒有很複雜，要加快應該能由減少資料量來著手。組員試著先做一次資料前處理，只保留 `action=order` 的資料，拿它來進行計算。真的夭壽快！！！


#### 資料減量思路

由於減量的 Web Logs 真的算起來很快，我們才確立了要以減量作為主要的最佳化方法。用一般的 grep 或寫程式過濾 Web Logs 看起來都速度不快，得設法克服這個問題。

倚賴外部程式做資料前處理，看起來不會比直接在程式內實作有效率。那麼我們該在哪邊進行資料前處理呢？對 Hadoop 來說，我們透過指定 InputFormatClass 來決定資料如何被讀進來：

```java
job.setInputFormatClass(TextInputFormat.class);
```

由 Hello World 內複製來的是使用一般的 `TextInputFormat`，它其實是以換行字元為單位讀檔的 InputFormat。先試做了第 1 版自製的 [org.ehc.inputv1.MyInputFormat](https://github.com/phstudy/ehc2015/blob/8c5d38a663565692c615028a702ab13599550e34/hadoop-itemcount/mapreduce/src/main/java/org/ehc/inputv1/MyInputFormat.java)，在 readLine 時動手腳，讓它只留下 `action=order` 的資料，由於資料減量了也有比原先跑得快了些，但是沒有到飛快的感覺。於是在先前的基礎上稍為改良一下，寫成 [org.ehc.inputv2.MyMergePlistInputFormat](https://github.com/phstudy/ehc2015/blob/8c5d38a663565692c615028a702ab13599550e34/hadoop-itemcount/mapreduce/src/main/java/org/ehc/inputv2/MyMergePlistInputFormat.java)，雖然同樣是只讀購買記錄的部分，但是把多筆 plist 合併成 1 組，試著減少 Mapper 被呼叫的機會，確實有再快一咪咪，進步的效果不太顯著。

資料減量的方向是對的，但為什麼效果不顯著呢？這問題讓我們苦思了幾天。先來看一下檔案行數：

```
qty:Downloads qrtt1$ wc -l EHC_1st_round.log
 5461323 EHC_1st_round.log
```

透過改寫的 InputFormat 已經減少了 Mapper 呼叫的次數，但是在 InputFormat 仍需要承受這麼多筆以「行」為單位的解析工作。若是我們不以「行」為單位是否會更有效率點呢？如果不以行為單位，那麼分隔的 token 選什麼比較好呢？順著這個思路，最後選擇了以 `action=order` 作為分隔點。實作了一個簡單的 Parser [util.OrderPlistFinder](https://github.com/phstudy/ehc2015/blob/8c5d38a663565692c615028a702ab13599550e34/hadoop-itemcount/mapreduce/src/main/java/util/OrderPlistFinder.java)，它是 [org.ehc.inputv3.ByteBufferOrderPlistInputFormat](https://github.com/phstudy/ehc2015/blob/8c5d38a663565692c615028a702ab13599550e34/hadoop-itemcount/mapreduce/src/main/java/org/ehc/inputv3/ByteBufferOrderPlistInputFormat.java) 主要的邏輯。由於購買記錄佔總筆數相當少，新實作的 InputFormat 就真的大部提昇了效率。


### 測試數據

在 EC2 的實驗環境中，最快可以跑到 18 秒多。不過，大部分時間是 19 ~ 22 秒間移動。

```
real	0m18.827s
user	0m39.045s
sys	0m3.333s
```

PS. 若不上傳至 HDFS 約 13 秒左右


### EC2 環境

由於執行環境是 EC2 的 m3.xlarge，依手冊的內容 [Choose an EC2 Instance with Enough Bandwidth](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-ec2-config.html)，能看出它的頻寬是 `62.5 MB/s`

```
qty:Downloads qrtt1$ ls -al |grep EH
-rw-r-----@   1 qrtt1  staff     2048513  4 27 23:50 15-04-18_EHC_2015_競賽說明.pdf
-rw-r-----@   1 qrtt1  staff   227152375  4 18 16:51 EHC_1st.tar.gz
-rw-r--r--@   1 qrtt1  staff  1656885703  4  8 16:10 EHC_1st_round.log
```

理論上的速度會在這個之間：

```
qty:Downloads qrtt1$ python
Python 2.7.6 (default, Sep  9 2014, 15:04:36)
[GCC 4.2.1 Compatible Apple LLVM 6.0 (clang-600.0.39)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
>>> 1656885703 / (62.5*1024*1024)
25.282069442749023
>>>
```

不過實際上單純將檔案傳入 HDFS 約 14 秒多，也許比賽的環境有些參數是我們沒有想到的（或是官方文件沒有更新!?）。




