package com.yourcompany.tables.master


import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.dataquality._
import org.apache.spark.sql.types._


class tbl_demo_taxi_mes(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable {
  /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/
  //Tipo de tabla, Master y Reference son catalogos sin particiones de periodo
  this.setTableType(huemulType_Tables.Transaction)
  //Base de Datos en HIVE donde sera creada la tabla
  this.setDataBase(huemulBigDataGov.GlobalSettings.MASTER_DataBase)
  //Tipo de archivo que sera almacenado en HDFS
  this.setStorageType(huemulType_StorageType.PARQUET)
  //Ruta en HDFS donde se guardara el archivo PARQUET
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.MASTER_SmallFiles_Path)
  //Ruta en HDFS especifica para esta tabla (Globalpaths / localPath)
  this.setLocalPath("demo_taxi/")
  //Frecuencia de actualización
  this.setFrequency(huemulType_Frequency.MONTHLY)
  //Permite guardar los errores y warnings en la aplicación de reglas de DQ, valor por default es true
  //this.setSaveDQResult(true)
  //Permite guardar backup de tablas maestras
  //this.setSaveBackup(true)  //default value = false
  
  /**********   O P T I M I Z A C I O N  ****************************************/
  //Indica la cantidad de particiones al guardar un archivo, para archivos pequeños (menor al bloque de HDFS) se 
  //recomienda el valor 1, mientras mayor la tabla la cantidad de particiones debe ser mayor para aprovechar el paralelismo
  //this.setNumPartitions(1)
  //setSaveDQErrorOnce: true (default). Guarda todos los detalles de error o warning de DQ en disco una sola vez (ejemplo: falla regla 1 y regla 2, escribe en disco una sola vez --> usar cuando hay suficiente memoria RAM para el proceso, ya que consolida todos los DF en uno solo)
  //                    false. Guarda en disco cada resultado de error o warning en forma independiente (ej: falla regla 1 y regla 2, escribe en disco 2 veces -- usar cuando hay poca mejora RAM para ejecutar el proceso) 
  this.setSaveDQErrorOnce(true)
  
  /**********   C O N T R O L   D E   C A M B I O S   Y   B A C K U P   ****************************************/
  //Permite guardar los errores y warnings en la aplicación de reglas de DQ, valor por default es true
  this.setSaveDQResult(true)
  //Permite guardar backup de tablas maestras
  //this.setSaveBackup(true)  //default value = false
  
    //columna de particion
  this.setPartitionField("periodo_mes")
  /**********   S E T E O   I N F O R M A T I V O   ****************************************/
  //Nombre del contacto de TI
  this.setDescription("Tabla que contiene los datos de taxis de NY")
  //Nombre del contacto de negocio
  this.setBusiness_ResponsibleName("Sebastián Rodríguez")
  //Nombre del contacto de TI
  this.setIT_ResponsibleName("Sebastián Rodríguez")
   
  /**********   D A T A   Q U A L I T Y   ****************************************/
  //DataQuality: maximo numero de filas o porcentaje permitido, dejar comentado o null en caso de no aplicar
  //this.setDQ_MaxNewRecords_Num(null)  //ej: 1000 para permitir maximo 1.000 registros nuevos cada vez que se intenta insertar
  //this.setDQ_MaxNewRecords_Perc(null) //ej: 0.2 para limitar al 20% de filas nuevas
    
  /**********   S E G U R I D A D   ****************************************/
  //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeFull_addAccess("process_taxi_mes", "com.yourcompany.demo_taxi")
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyInsert_addAccess("[[MyclassName]]", "[[my.package.path]]")
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyUpdate_addAccess("[[MyclassName]]", "[[my.package.path]]")
  

  /**********   C O L U M N A S   ****************************************/

    //Columna de period
  val periodo_mes = new huemul_Columns (StringType, true,"periodo de los datos").setIsPK()
  val UniqueKey = new huemul_Columns (StringType, true,"Clave generada sha2").setIsPK()
    
  val VendorID = new huemul_Columns (IntegerType, true, "VendorID").setNullable("").securityLevel(huemulType_SecurityLevel.Public)  
  val tpep_pickup_datetime = new huemul_Columns (StringType, true, "tpep_pickup_datetime").setNullable("").securityLevel(huemulType_SecurityLevel.Public)  
  val tpep_dropoff_datetime = new huemul_Columns (StringType, true, "tpep_dropoff_datetime").setNullable("").securityLevel(huemulType_SecurityLevel.Public)  
  val passenger_count = new huemul_Columns (IntegerType, true, "passenger_count").setNullable("").securityLevel(huemulType_SecurityLevel.Public)  
  val trip_distance = new huemul_Columns (DecimalType(6,4), true, "trip_distance").setNullable("").securityLevel(huemulType_SecurityLevel.Public)  
  val RatecodeID = new huemul_Columns (IntegerType, true, "RatecodeID").setNullable("").securityLevel(huemulType_SecurityLevel.Public)  
  val store_and_fwd_flag = new huemul_Columns (StringType, true, "store_and_fwd_flag").setNullable("").securityLevel(huemulType_SecurityLevel.Public) 
  val PULocationID = new huemul_Columns (IntegerType, true, "PULocationID").setNullable("").securityLevel(huemulType_SecurityLevel.Public) 
  val DOLocationID = new huemul_Columns (IntegerType, true, "DOLocationID").setNullable("").securityLevel(huemulType_SecurityLevel.Public) 
  val payment_type = new huemul_Columns (IntegerType, true, "payment_type").setNullable("").securityLevel(huemulType_SecurityLevel.Public) 
  val fare_amount = new huemul_Columns (IntegerType, true, "fare_amount").setNullable("").securityLevel(huemulType_SecurityLevel.Public) 
  val extra = new huemul_Columns (DecimalType(6,4), true, "extra").setNullable("").securityLevel(huemulType_SecurityLevel.Public)  
  val mta_tax = new huemul_Columns (DecimalType(6,4), true, "mta_tax").setNullable("").securityLevel(huemulType_SecurityLevel.Public)  
  val tip_amount = new huemul_Columns (DecimalType(6,4), true, "tip_amount").setNullable("").securityLevel(huemulType_SecurityLevel.Public) 
  val tolls_amount = new huemul_Columns (DecimalType(6,4), true, "tolls_amount").setNullable("").securityLevel(huemulType_SecurityLevel.Public) 
  val improvement_surcharge = new huemul_Columns (DecimalType(6,4), true, "improvement_surcharge").setNullable("").securityLevel(huemulType_SecurityLevel.Public)  
  val total_amount = new huemul_Columns (DecimalType(6,4), true, "total_amount").setNullable("").securityLevel(huemulType_SecurityLevel.Public) 
  val congestion_surcharge = new huemul_Columns (StringType, true, "congestion_surcharge").setNullable("").securityLevel(huemulType_SecurityLevel.Public)

   
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  //val i[[tbl_PK]] = new [[tbl_PK]](huemulBigDataGov,Control)
  //val fk_[[tbl_PK]] = new huemul_Table_Relationship(i[[tbl_PK]], false)
  //fk_[[tbl_PK]].AddRelationship(i[[tbl_PK]].[[PK_Id]], [[LocalField]_Id)
    
  //**********Ejemplo para agregar reglas de DataQuality Avanzadas  -->ColumnXX puede ser null si la validacion es a nivel de tabla
  //**************Parametros
  //********************  ColumnXXColumna a la cual se aplica la validacion, si es a nivel de tabla poner null
  //********************  Descripcion de la validacion, ejemplo: "Consistencia: Campo1 debe ser mayor que campo 2"
  //********************  Formula SQL En Positivo, ejemplo1: campo1 > campo2  ;ejemplo2: sum(campo1) > sum(campo2)  
  //********************  CodigoError: Puedes especificar un codigo para la captura posterior de errores, es un numero entre 1 y 999
  //********************  QueryLevel es opcional, por default es "row" y se aplica al ejemplo1 de la formula, para el ejmplo2 se debe indicar "Aggregate"
  //********************  Notification es opcional, por default es "error", y ante la aparicion del error el programa falla, si lo cambias a "warning" y la validacion falla, el programa sigue y solo sera notificado
  //********************  SaveErrorDetails es opcional, por default es "true", permite almacenar el detalle del error o warning en una tabla específica, debe estar habilitada la opción DQ_SaveErrorDetails en GlobalSettings
  //********************  DQ_ExternalCode es opcional, por default es "null", permite asociar un Id externo de DQ
  val DQ_total_amount_notnull: huemul_DataQuality = new huemul_DataQuality(total_amount,"total_amount not null", "total_amount is not null",1,huemulType_DQQueryLevel.Row, huemulType_DQNotification.WARNING, true, "EX-CODE-01")
    
  this.ApplyTableDefinition()
}

