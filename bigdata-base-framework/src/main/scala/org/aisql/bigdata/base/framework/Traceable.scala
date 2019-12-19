package org.aisql.bigdata.base.framework

import org.aisql.bigdata.base.framework.bean.RegressBean
import org.aisql.bigdata.base.framework.enums.ErrorMsg
import org.aisql.bigdata.base.util.DateUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Author: xiaohei
  * Date: 2019/12/19
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
trait Traceable[B] {
  /**
    * 可回溯接口,有执行全量、回溯逻辑能力
    *
    * @param data           从service中得到的数据
    * @param selectKeyFunc  入参为对象,返回该对象需要组成idkey的字段组合,多个字段必须使用separator指定的分隔符拼接
    * @param selectTimeFunc 入参为对象,返回该对象需要和回溯时间比对的时间字段
    * @param separator      idkey拼接的分隔符,默认为下划线"_"
    * @param regressOrNot   传入回溯样本,无则执行全量计算
    * @return 全量数据集或者根据样本表过滤后的回溯数据集
    **/
  def doBusiness(data: RDD[B],
                 selectKeyFunc: (B) => String,
                 selectTimeFunc: (B) => String,
                 separator: String = "_",
                 regressOrNot: Option[RegressBean] = None
                )(implicit env: SparkSession, vt: ClassTag[B]): RDD[(String, Iterable[B])] = {
    val sampleOrNot = if (regressOrNot.nonEmpty) {
      val regressInfo = regressOrNot.get
      val sampleRdd = env.table(regressInfo.sampleTable).rdd.map(x =>
        (x.getAs[String](regressInfo.sampleIdCardField) + separator + x.getAs[String](regressInfo.sampleCardNameField),
          x.getAs[String](regressInfo.loanDayField))).groupByKey()

      if (sampleRdd.count() <= regressInfo.maxCount) {
        Right(Some(env.sparkContext.broadcast(
          sampleRdd
            .collect()
            .toMap
        )))
      } else {
        Left(s"${ErrorMsg.gtMaxCnt} ${regressInfo.maxCount}")
      }
    } else {
      Right(None)
    }

    sampleOrNot match {
      case Right(brdSample) =>
        brdSample match {
          //执行回溯
          case Some(regressSample) =>
            //过滤人
            data.filter(x => regressSample.value.contains(selectKeyFunc(x)))
              //过滤时间并处理同个人多个时间的情况
              .flatMap { x =>
              val idkey = selectKeyFunc(x)
              val loanDays = regressSample.value(idkey)
              loanDays.map(d => (idkey + separator + d, x))
            }.groupByKey()
              //过滤时间
              .map {
              x =>
                val (key, value) = (x._1, x._2)
                val loadDay = key.split(separator).last
                //数据时间小于回溯时间(适用于t+1实时对比),否则需要为小于等于
                (key, value.filter(x => DateUtil.daysDiff(selectTimeFunc(x), loadDay) < 0))
            }.filter(_._2.nonEmpty)
          //执行全量
          case None =>
            data.map(x => (selectKeyFunc(x), x)).groupByKey()
        }
      case Left(errMsg) => throw new Exception(errMsg)
    }
  }
}
