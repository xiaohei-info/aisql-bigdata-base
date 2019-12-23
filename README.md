bigdata-base 为 **大数据业务开发** 基准项目。

主要分为两个子模块：

- framwork：大数据业务开发项目模板，基于此模板可 **快速开发 Spark、Flink 等业务项目**。
- component：提供基础通用的 **外部数据链接器、多类型加解密包、压缩、文件、日期操作等工具类函数**。

## 一、framework

```org.aisql.bigdata.base.framework``` 包中提供了几种常见大数据项目需要用到的数据源。

framework 以 **模块化项目** 的结构提供了 **各个数据源基础的Dao、Service接口与默认实现**。

由于 framework 为业务项目提供了 **结构化标准**，其可以规整所有 **混乱的业务项目结构**。

基于标准结构化，framework还能够提供 **代码生成工具**，把开发人员从繁琐的、具体的、重复的编码工作中解放出来。

项目结构标准化的重要性：

- **项目统一管理与生成**
- **方便快速搭框架**
- **所有开发人员遵守相同的编码规范**
- **易于交接与维护**

使用 framework，开发人员能够做到开箱即用，不必再花太多精力在研究计算引擎与各个数据源的接口和API如何调用，**专注于业务逻辑的实现，提升开发效率**。

framework 目前支持的数据源包括以下几种：

- hive
- hdfs（开发中）
- hbase（开发中）
- kafka（开发中）

### 1.1 使用步骤

#### 1.1.1 基础包引用

maven引用：

```xml
<dependency>
    <groupId>org.aisql.bigdata</groupId>
    <artifactId>bigdata-base-framework</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

源码编译：

```shell
git clone https://github.com/chubbyjiang/aisql-bigdata-base.git
cd aisql-bigdata-base
mvn package 
```

将生产的jar包设置到项目依赖环境中。

或者可以直接从 github release 界面下载最新的发布包。

#### 1.1.2 计算引擎环境设置

由于 framework 依赖于具体的计算引擎环境，如Spark、Flink等，故在使用 framework 前需要将对应的开发环境设置完毕。

### 1.2 业务数据接口定义

由于基础接口是泛型的，需要有具体类型的子类实现否则无法使用。

所以要使用 framework，除了项目提供的基础接口之外，还需要开发人员在相应的模块中，根据业务方面定义相关的类实现基础接口中定义的方法：

- Bean：即Java POJOs，与数据源表一一对应
- Dao：与Bean一一对应，继承相关的基础接口（如：SparkBaseHiveDaoImpl）并实现其抽象函数
- Service：与Dao一一对应，继承相关的基础接口（如：BaseHiveService）并实现其抽象变量

具体的定义过程参考 **「1.5 自定义代码」**。

由于上述几种类型的定义工作比较繁琐，开发人员需要做的重复性工作比较多，让开发人员做更多的工作并不是 framework 的目的。

基于 framework 标准结构化，可以比较容易的通过 **代码生成器** 来自动生成代码文件并使用，提高开发人员的工作效率。

### 1.3 代码自动生成

使用代码生成器可以一键生成所有数据源（表）对应的 Bean、Dao与Service，即一键生成所有数据访问操作，将生成的文件复制黏贴到项目中即可使用。

代码生成器代号：gojira，代码包：```org.aisql.bigdata.base.gojira```

使用方式：

```scala
import org.aisql.bigdata.base.gojira.Gojira
import org.aisql.bigdata.base.gojira.enum.EngineType

val savePath = "/Users/xiaohei/Downloads/tmp/test"
val projectName = "framework-demo"
//项目顶级包名
val basePackage = "org.aisql.bigdata.base.framework.demo"
//项目作者
val whoami = "xiaohei"
//需要操作的数据源，以hive表为例
val tables = "default.t_users,default.t_classes".split(",")
val gojira = new Gojira(savePath, projectName, basePackage, whoami)
//根据执行引擎生成对应的代码文件
gojira.setMonster(EngineType.SPARK)
//设置表并初始化其表结构信息
gojira.setTable(tables, spark)
//保存到指定路径下，zip压缩包
gojira.save("org.aisql.bigdata"
```

在能够连接到 hive metastore 的环境中配置依赖包（比如spark-shell）并执行以上代码，将会得到一个 **${项目名}.zip** 的压缩包，解压开将其中的文件（包含了所有表对应的Bean、Dao与Service）贴到项目中即可开始使用。

需要注意的是 **hive表中的字段名必须为下划线格式**, 驼峰类型的格式由于hive中不识别大小写到最最后读取的字段名都为小写。

默认情况下，hive表的字段名会原封不动地映射为实体类的字段名，如果需要将hive表下划线形式转换为实体类的驼峰形式，可以通过以下参数设置:

```scala
gojira.setTable(tables, spark, toCamel = true)
```

### 1.4 数据接口使用

现有 default.t_users 表需要读取，代码如下：

```scala
implicit val env:SparkSession = spark
val service = new UsersService
//读取整个表
val allRdd:RDD[Users] = service.selectAll()
//字段筛选
val allRdd:RDD[Users] = service.selectAllWithCols(Seq("name","age"))
//条件过滤
val allRdd:RDD[Users] = service.selectAllByWhere("age>10")
//读取1000条数据
val demoRdd:RDD[Users] = service.selectDemo()
//读取hdfs上的文本文件
val fromTextBean:RDD[Users] = service.fromTextFile(",")
//写入表
service.insertInto(demoRDD)
service.createTable(demoRDD)
```

相关数据操作的API见 ```org.aisql.bigdata.base.framework.hive.BaseHiveService```

### 1.5 回溯接口使用

为分离数据处理逻辑与业务处理逻辑，统一回溯处理方式，framework 中为业务方提供了现成的回溯接口 ```Traceable```。

默认情况下，Gojira 生成的 Service 实现类将会集成这个特质接口，使其拥有回溯能力(目前支持Spark类)。

业务方使用方式如下:

```scala
val service = new OdsCreditTradinglogHISparkHiveService


import org.aisql.bigdata.base.framework.bean.RegressBean
//样本表名、样本表id字段名、样本表name字段名、样本表回溯时间字段名、最大数据量、全量表id字段名、全量表name字段名
val regress = RegressBean("default.sample_test", "id", "name", "date", 100000, "id_card_no", "card_name")

//执行回溯
service.doBusiness(
  service.selectAll(),
  x => x.id_card_no + "_" + x.card_name,
  x => x.create_time,
  separator = "_",
  regressOrNot = Some(regress)
)

//执行全量
service.doBusiness(
  service.selectAll(),
  x => x.id_card_no + "_" + x.card_name,
  x => x.create_time,
  separator = "_",
  regressOrNot = None
)
```

其中，如果 ```regressOrNot = None``` 则会执行全量逻辑，效果等同 ```service.selectAll().groupByKey()```

### 1.6 自定义代码

为了使用户了解如何通过 framework 提供的基础接口定义 Bean、Dao与Service 来实现对应的数据操作方法，下面将会对基础接口的开发过程进行介绍。

以 **Spark连接Hive** 为例，仍然是 default.t_users 表。

1、创建 Users 类与之对应：

```scala
class Users {
    var name: String = _
    var age: Int = _
}
```

2、创建 Dao 类提供数据读写操作：

```scala
class UsersDao extends SparkBaseHiveDaoImpl[Users] {
     //表名
     override val TABLE: String = "t_users"
     //数据库名
     override val DATABASE: String = "default"
     //全表名
     override val FULL_TABLENAME: String = s"$DATABASE.$TABLE"
     //表的hdfs存储路径
     override val HDFS_PATH: String = s"/user/hive/warehouse/$DATABASE.db/$TABLE"

     /**
        * 读取hive数据时,将DadaFrame的Row转化为具体的bean对象
        *
        * @param df Row对象
        * @return 具体的bean对象
        **/
     override protected def transDf2Rdd(df: DataFrame)(implicit env: SparkSession): RDD[Users] = {

     }

     /**
        * 写入hive表时,将RDD转换为DataFrame
        *
        * @param rdd rdd对象
        * @return DataFrame对象
        **/
     override protected def transRdd2Df(rdd: RDD[Users])(implicit env: SparkSession): DataFrame = {

     }

    /**
       * 读取hdfs text文件时,将文本数据(数组)转化为具体的bean对象
       *
       * @param arrRdd 使用分隔符split之后的数据数组
       * @return 具体的bean对象
       **/
     override protected def transText2Bean(arrRdd: RDD[Array[String]]): RDD[Users] = {

     }
}
```

Dao 通过开发人员提供的 ```transDf2Rdd、transRdd2Df``` 函数来实现 Row 到 Pojo 的相互转换。

为了减少繁琐的类型转化操作，framework中提供了 **通过反射实现 Row 到 Pojo 相互转换的工具类**。

开发人员可以使用 ```org.aisql.bigdata.base.framework.util.DataFrameReflactUtil``` 来快速实现这个功能。

```scala
override protected def transDf2Rdd(df: DataFrame)(implicit env: SparkSession): RDD[Users] = {
    df.rdd.map {
      row =>
        //反射创建pojo对象，读取row数据内容并设置到pojo中
        DataFrameReflactUtil.generatePojoValue(classOf[Users], row).asInstanceOf[Users]
    }
}

override protected def transRdd2Df(rdd: RDD[Users])(implicit env: SparkSession): DataFrame = {
    val structs = DataFrameReflactUtil.getStructType(classOf[Users]).get
    //反射创建row对象，并从pojo中读取字段内容与类型设置到row对象中
    val rowRdd = rdd.flatMap(r => DataFrameReflactUtil.generateRowValue(classOf[Users], r))
    env.createDataFrame(rowRdd, structs)
}

override protected def transText2Bean(arrRdd: RDD[Array[String]]): RDD[Users] = {
  arrRdd.map(r => DataFrameReflactUtil.generatePojoValue(classOf[Users], r).asInstanceOf[Users])
}
```


3、创建 Service 类提供业务操作接口，实现dao的实例化：

```scala
class UsersService extends BaseHiveService[SparkSession, RDD[Uesrs]] {
    protected override val dao: BaseHiveDao[SparkSession, RDD[Uesrs]] = new UsersDao
}
```

4、业务使用同**「1.4 数据接口使用」**小节

### 1.7 数据源开发

由于各个数据源的读取方式与提供给业务层使用的读取接口各不相同，如 BaseHiveDao 专门处理Hive数据读写，会提供 fromHive、createTable等操作，所以不同的数据源需要独立开发。

数据源开发参与方式如下，欢迎贡献代码。

 - 在 ```org.aisql.bigdata.base.framework``` 下新建以数据源为名的包，如：kafka
 - 创建对应的基础Dao与Service接口类，并继承 ```Daoable``` 或者 ```Serviceable```，如：BaseKafkaDao、BaseKafkaService
 - 在基础类中定义数据操作接口

```Daoable``` 与 ```Serviceable``` 目前并没有任何定义，只作为标记接口使用，后续需要Dao与Service整合到该接中以统一数据源开发规范。

