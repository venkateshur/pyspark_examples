from pyspark.sql.functions import *

from model.FileReader import textReader, csvReader
from conf import AppConf


class SrcFiles:
  def __init__(self, tocorganization, tocpartycontractrole, toccontractstub, todomaininstance,tocpointofcontact, lookupgroupind, mstr_org_tbl ):
    self.tocorganization = tocorganization
    self.itocpartycontractrole = tocpartycontractrole
    self.toccontractstub = toccontractstub
    self.todomaininstance = todomaininstance
    self.tocpointofcontact = tocpointofcontact
    self.lookupgroupind = lookupgroupind
    self.mstr_org_tbl = mstr_org_tbl


class Process:
    def apply(appConf, spark):
        readInputFilesList = textReader(spark, appConf.inputFilesPath).collect()
        inputFiles = readInputFilesList.map(lambda inputPath: csvReader(spark, appConf.inputBasePath + "/" + inputPath))
        buildInput = buildInputDataSets(inputFiles)
        resjoin6befFilter = join6befFilter(joinTocOrgWithToc(buildInput.tocorganization, buildInput.tocpartycontractrole,
                                                             buildInput.toccontractstub, buildInput.todomaininstance), selGrpCntc(buildInput.tocpointofcontact))
        resselCol = selCol(resjoin6befFilter)
        reslkpGrpInd = lkpGrpInd(buildInput.lookupgroupind, buildInput.mstr_org_tbl)
        resjoinLkpGrpInd = joinLkpGrpInd(resselCol, reslkpGrpInd)
        resfilterLkpGrpIndnulls = filterLkpGrpIndnulls(resjoinLkpGrpInd)
        resrejLkpGrpInd = rejLkpGrpInd(resjoinLkpGrpInd)
        rescapRejLkpGrpInd = capRejLkpGrpInd(resrejLkpGrpInd, reslkpGrpInd)
        resfunnelMstrPrty = funnelMstrPrty(resfilterLkpGrpIndnulls,rescapRejLkpGrpInd)
        reswriteContact = writeContact(spark, resfunnelMstrPrty)
        reswriteToOutput =  writeToOutput(reswriteContact, appConf.outputFilesPath + "/" + appConf.ouputFileName)


    def buildInputDataSets(readSrcFiles):
        if __name__ == '__main__':
                return SrcFiles(readSrcFiles[0]
                                .withColumn('toc_org_CABC', expr("CABC"))
                                .withColumn('toc_org_IABC', expr("IABC").withColumn('toc_org_NameABC', expr("NAMEABC"))).withColumn("toc_org_CORPORATETAXNABC", expr("CORPORATETAXNABC"))
                                .withColumn("toc_org_LASTUPDATEDATEABC", expr("LASTUPDATEDATEABC")).drop('CABC', 'IABC', 'NAMEABC', 'CORPORATETAXNABC', 'STARTDATEABC', 'LASTUPDATEDATEABC')
                                .select('toc_org_CABC','toc_org_IABC','toc_org_NameABC','toc_org_CORPORATETAXNABC','toc_org_LASTUPDATEDATEABC','PARTYTYPEABC'),
                                readSrcFiles[1]
                                .select(expr("C_OCPRTY_CONTRACTROLESABC"),expr("I_OCPRTY_ContractRolesABC"),expr("ROLEABC"),expr("C_OCCTRSTB_CONTRACTROLESABC"),expr("I_OCCTRSTB_CONTRACTROLESABC")),
                                readSrcFiles[2]
                                .withColumn('toc_con_stb_ENDDATEABC', expr("ENDDATEABC")).withColumn("toc_con_stb_STARTDATEABC", expr("STARTDATEABC")).withColumn("toc_con_stb_CABC", expr("CABC")).withColumn("toc_con_stb_IABC", expr("IABC")).drop('ENDDATEABC', 'STARTDATEABC', 'CABC', 'IABC')
                                .select(expr("toc_con_stb_ENDDATEABC"),expr("toc_con_stb_STARTDATEABC"),expr("toc_con_stb_CABC"),expr("toc_con_stb_IABC"), expr("CONTRACTSTATUABC"),expr("ReferenceNumbABC")),
                                readSrcFiles[3]
                                .select(expr("IABC"), expr ("NAMEABC")),
                                readSrcFiles[4],
                                readSrcFiles[5],
                                readSrcFiles[6])



  def joinTocOrgWithToc(df_tocorganization, df_tocpartycontractrole, df_toccontractstub,df_todomaininstance):
      return df_tocorganization.join(df_tocpartycontractrole, df_tocorganization["toc_org_CABC"] == df_tocpartycontractrole["C_OCPRTY_CONTRACTROLESABC"] & df_tocorganization["toc_org_IABC"] == df_tocpartycontractrole["I_OCPRTY_CONTRACTROLESABC"], "left")\
          .join(df_toccontractstub, df_tocpartycontractrole["C_OCCTRSTB_CONTRACTROLESABC"] == df_toccontractstub["toc_con_stb_CABC"] & df_tocpartycontractrole["I_OCCTRSTB_CONTRACTROLESABC"] == df_toccontractstub["toc_con_stb_IABC"], "left")\
          .join(df_todomaininstance, col("CONTRACTSTATUABC") == df_todomaininstance("IABC"), "left").withColumnRenamed("NAMEABC", "GRP_CNTRCT_STATUSABC").drop("IABC")\
          .join(df_todomaininstance, col("PARTYTYPEABC") == df_todomaininstance("IABC"), "left").withColumnRenamed("NAMEABC", "PRTY_TYPABC").drop("IABC")\
          .join(df_todomaininstance, col("ROLEABC") == col("IABC"), "left").withColumnRenamed("NAMEABC", "PRTY_ROLEABC")



  def selGrpCntc(df_tocpointofcontact):
    ds =  df_tocpointofcontact\
      .withColumn("C_OCPRTY_POINTSOFCONTAABC", when(col("C_OCPRTY_POINTSOFCONTAABC") == "NULL", 0).otherwise(col("C_OCPRTY_POINTSOFCONTAABC")))\
      .withColumn("EFFECTIVEFROMABC", when(col("EFFECTIVEFROMABC") == "NULL", "1800-01-01 00:00:00.000").otherwise("EFFECTIVEFROMABC"))\
      .withColumn("EFFECTIVETOABC", when(col("EFFECTIVETOABC") == "NULL", "2100-01-01 00:00:00.000").otherwise("EFFECTIVETOABC"))\
      .filter(col("C_OCPRTY_POINTSOFCONTAABC") > "0"& current_timestamp().between(col("EFFECTIVEFROMABC"), col("EFFECTIVETOABC")))\
      .select( col("C_OCPRTY_POINTSOFCONTAABC"), col("I_OCPRTY_POINTSOFCONTAABC"), col("CONTACTNAMEABC"))\
      .groupBy("C_OCPRTY_POINTSOFCONTAABC", "I_OCPRTY_POINTSOFCONTAABC").agg(max("CONTACTNAMEABC").alias("GRP_CNTCABC")).cache()

    return ds



  def join6befFilter(finalDf, selGrpCntc):
    join6befFilter = finalDf.join(selGrpCntc, finalDf("toc_org_CABC") == selGrpCntc("C_OCPRTY_POINTSOFCONTAABC") & finalDf("toc_org_IABC") == selGrpCntc("I_OCPRTY_POINTSOFCONTAABC"), "left")\
      .select(finalDf("toc_org_CABC"), finalDf("toc_org_IABC"), finalDf("REFERENCENUMBABC"), finalDf("toc_org_NameABC").alias("ACCT_NMABC"), finalDf("toc_org_CORPORATETAXNABC").alias("GRP_TAX_IDABC")
        , finalDf("toc_con_stb_STARTDATEABC").alias("GRP_START_DTABC")
        , finalDf("toc_con_stb_ENDDATEABC").alias("GRP_TERM_DTABC"), finalDf("GRP_CNTRCT_STATUSABC"), finalDf("PRTY_TYPABC"), finalDf("PRTY_ROLEABC"), finalDf("toc_org_LASTUPDATEDATEABC")
        , selGrpCntc("GRP_CNTCABC"))

    return join6befFilter.filter(col("REFERENCENUMBABC").isNotNull)

@udf("string")
def convert(x):
        if x.indexOfSlice(":") == -1:
            return x.substring(0, x.length)
        else:
            return x.substring(0, x.indexOfSlice(":") - 0)

@udf("string")
def cnvCharIndex(x):
    if x.indexOfSlice(":") == -1:
        return ' '
    else:
        return x.substring(x.indexOfSlice(":") + 1, len(x))

def selCol(join6):
    selCol = join6.select(col("toc_org_CABC").alias("ORG_CABC"), col("toc_org_IABC").alias("ORG_IABC"),
                          when(col("toc_org_IABC").isNull, "")
                          .when(col("toc_org_IABC") > 0, concat_ws("~", col("toc_org_CABC"), col("toc_org_IABC"))).alias("PRTY_KEYABC")
                          , convert(col("REFERENCENUMBABC")).alias("PRTY_GRP_NBRABC"),
                          cnvCharIndex(col("REFERENCENUMBABC")).alias("PRTY_ACCT_NBRABC"),
                          col("ACCT_NMABC").alias("PRTY_ACCT_NMABC"), col("GRP_TAX_IDABC").alias("PRTY_TAX_IDABC")
                          , col("GRP_START_DTABC").alias("PRTY_EFF_DTTMABC")
                          , col("GRP_TERM_DTABC").alias("PRTY_END_DTTMABC"), col("GRP_CNTRCT_STATUSABC").alias("PRTY_STUTSABC")
                          , col("PRTY_TYPABC"), col("PRTY_ROLEABC"),
                          col("toc_org_LASTUPDATEDATEABC")
                          , col("GRP_CNTCABC").alias(("PRTY_CONTC_NMABC"))
                          , lit(None).alias("PRTY_CONT_NMABC")
                          , lit(None).alias("PRTY_ERISA_INRABC")

    return selCol



  def lkpGrpInd(df_lookupgroupind, df_mstr_org_tbl):
    return df_lookupgroupind.join(df_mstr_org_tbl, trim(df_lookupgroupind("CURR_CDABC")) == trim(df_mstr_org_tbl("ORG_NMABC")) & df_mstr_org_tbl("ORG_STUSABC") == "ACTIVE", "left")\
      .select(trim(col("GROUP_NOABC")).cast("string").alias("GROUP_NOABC"), trim(col("ACCT_NOABC")).cast("string").alias("ACCT_NOABC"), col("GROUP_NMABC"), col("CURR_CDABC"), col("MKT_SEGMNTABC"), col("SITUS_STABC"), \
              col("BOCABC"), col("ACCT_MGRABC"), col("ACCT_EXEC_NMABC"), col("SALES_CDABC"), \
              col("RATING_STABC"), col("BILL_STABC"), col("NO_OF_LVSABC"), col("SRCABC"), \
              col("ORG_SEQ_IDABC"), col("PRTY_FED_IDABC"), col("PRTY_SIC_CDABC"), col("PRTY_SIC_CD_DESCABC"), \
              col("PRTY_SLS_CDABC"), col("PRTY_SLS_REP_NMABC"))


  def joinLkpGrpInd(selCol, lkpGrpInd):
    return selCol.join(lkpGrpInd, selCol("PRTY_GRP_NBRABC") == lkpGrpInd("GROUP_NOABC") & selCol("PRTY_ACCT_NBRABC") == lkpGrpInd("ACCT_NOABC"), "left")\
      .select(col("PRTY_KEYABC")
        , col("PRTY_GRP_NBRABC")
        , col("GROUP_NMABC").alias("PRTY_GRP_NMABC")
        , col("PRTY_ACCT_NBRABC"),
        col("PRTY_ACCT_NMABC")
        , col("PRTY_TYPABC"), col("PRTY_ROLEABC")
        , col("PRTY_STUTSABC")
        , col("PRTY_TAX_IDABC")
        , col("PRTY_EFF_DTTMABC")
        , col("PRTY_END_DTTMABC")
        , col("PRTY_CONTC_NMABC")
        , col("MKT_SEGMNTABC").alias("PRTY_MRK_SEGT_TYPEABC")
        , col("BOCABC").alias("PRTY_BENF_OFFICEABC")
        , col("BILL_STABC").alias("PRTY_BILL_STABC")
        , col("RATING_STABC").alias("PRTY_RATE_STABC")
        , lit(None).alias("PRTY_CONT_NMABC")
        , lit(None).alias("PRTY_ERISA_INRABC"))


  def filterLkpGrpIndnulls(joinLkpGrpInd):
    return joinLkpGrpInd.filter(col("PRTY_GRP_NMABC").isNotNull)

  def rejLkpGrpInd(joinLkpGrpInd):
    return joinLkpGrpInd.select(col("PRTY_KEYABC"), col("PRTY_GRP_NBRABC"), col("PRTY_GRP_NMABC"), col("PRTY_ACCT_NBRABC"),
                                col("PRTY_ACCT_NMABC"), col("PRTY_TYPABC"), col("PRTY_ROLEABC"), col("PRTY_STUTSABC"), col("PRTY_TAX_IDABC")
                                , col("PRTY_EFF_DTTMABC"), col("PRTY_END_DTTMABC"), col("PRTY_CONTC_NMABC"), col("PRTY_MRK_SEGT_TYPEABC")
                                , col("PRTY_BENF_OFFICEABC"), col("PRTY_BILL_STABC"), col("PRTY_RATE_STABC"), col("PRTY_CONT_NMABC")
                                , col("PRTY_ERISA_INRABC")).filter(col("PRTY_GRP_NMABC").isNull)


  def capRejLkpGrpInd(rejLkpGrpInd, lkpGrpInd):
    return rejLkpGrpInd.join(lkpGrpInd, rejLkpGrpInd("PRTY_GRP_NBRABC") == lkpGrpInd("GROUP_NOABC"), "Inner")\
      .select(col("PRTY_KEYABC")
        , col("PRTY_GRP_NBRABC")
        , col("PRTY_GRP_NMABC")
        , col("PRTY_ACCT_NBRABC"),
        col("PRTY_ACCT_NMABC")
        , col("PRTY_TYPABC"), col("PRTY_ROLEABC")
        , col("PRTY_STUTSABC")
        , col("PRTY_TAX_IDABC")
        , col("PRTY_EFF_DTTMABC")
        , col("PRTY_END_DTTMABC")
        , col("PRTY_CONTC_NMABC")
        , col("PRTY_MRK_SEGT_TYPEABC")
        , col("PRTY_BENF_OFFICEABC")
        , col("PRTY_BILL_STABC")
        , col("PRTY_RATE_STABC")
        , col("PRTY_CONT_NMABC")
        , col("PRTY_ERISA_INRABC"))



  def funnelMstrPrty(filterLkpGrpIndnulls, capRejLkpGrpInd):
      return filterLkpGrpIndnulls.union(capRejLkpGrpInd)


  def writeContact(sparkSession, funnelMstrPrty):
    writeContact = funnelMstrPrty.select(concat_ws("|", "PRTY_KEYABC", "PRTY_GRP_NBRABC", "PRTY_GRP_NMABC", "PRTY_ACCT_NBRABC","PRTY_ACCT_NMABC", "PRTY_TYPABC", "PRTY_ROLEABC", "PRTY_STUTSABC", "PRTY_TAX_IDABC", "PRTY_EFF_DTTMABC", "PRTY_END_DTTMABC", "PRTY_CONTC_NMABC", "PRTY_MRK_SEGT_TYPEABC", "PRTY_BENF_OFFICEABC", "PRTY_BILL_STABC", "PRTY_RATE_STABC", "PRTY_CONT_NMABC", "PRTY_ERISA_INRABC"))
    #val writeContact = finalDf.select( "toc_org_CABC","toc_org_IABC","toc_org_NameABC","toc_org_CORPORATETAXNABC","toc_org_LASTUPDATEDATEABC","toc_con_stb_ENDDATEABC","toc_con_stb_STARTDATEABC","GRP_CNTRCT_STATUSABC","PRTY_TYPABC","PRTY_ROLEABC")
    writeContact.show(5, False)
    return writeContact



  def writeToOutput(writeContact, OutputValue):
    Writer.csvWriter(writeContact, OutputValue)