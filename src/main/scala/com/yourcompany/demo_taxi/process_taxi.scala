package com.yourcompany.demo_taxi

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import java.util.Calendar;
import org.apache.spark.sql.types._
import com.yourcompany.tables.master._
import com.yourcompany.demo_taxi.datalake._
import com.yourcompany.settings.globalSettings
import org.apache.spark.storage.StorageLevel
import com.huemulsolutions.bigdata.tables.huemul_TableConnector
//import com.huemulsolutions.bigdata.tables._
//import com.huemulsolutions.bigdata.dataquality._


object process_taxi {
  
  /**
   * Este codigo se ejecuta cuando se llama el JAR desde spark2-submit. el codigo esta preparado para hacer reprocesamiento masivo.
  */
  def main(args : Array[String]) {
    //Creacion API
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Masterizacion tabla tbl_demo_taxi - ${this.getClass.getSimpleName}", args, globalSettings.Global)
    
    /*************** PARAMETROS **********************/
    var param_year = huemulBigDataGov.arguments.GetValue("year", null, "Debe especificar el parametro año, ej: year=2017").toInt
    var param_month = huemulBigDataGov.arguments.GetValue("month", null, "Debe especificar el parametro month, ej: month=12").toInt
     
    var param_day = 1
    val param_numMonths = huemulBigDataGov.arguments.GetValue("num_months", "1").toInt

    /*************** CICLO REPROCESO MASIVO **********************/
    var i: Int = 1
    var FinOK: Boolean = true
    var Fecha = huemulBigDataGov.setDateTime(param_year, param_month, param_day, 0, 0, 0)
    
    while (i <= param_numMonths) {
      param_year = huemulBigDataGov.getYear(Fecha)
      param_month = huemulBigDataGov.getMonth(Fecha)
      huemulBigDataGov.logMessageInfo(s"Procesando Año $param_year, month $param_month ($i de $param_numMonths)")
      
      //Ejecuta codigo
      var finControl = process_master(huemulBigDataGov, null, param_year, param_month)
      
      if (finControl.Control_Error.IsOK())
        i+=1
      else {
        huemulBigDataGov.logMessageError(s"ERROR Procesando Año $param_year, month $param_month ($i de $param_numMonths)")
        i = param_numMonths + 1
      }
        
      Fecha.add(Calendar.MONTH, 1)      
    }
    
    
    huemulBigDataGov.close
  }
  
  /**
    masterizacion de archivo [[CAMBIAR]] <br>
    param_year: año de los datos  <br>
    param_month: mes de los datos  <br>
   */
  def process_master(huemulBigDataGov: huemul_BigDataGovernance, ControlParent: huemul_Control, param_year: Integer, param_month: Integer): huemul_Control = {
    val Control = new huemul_Control(huemulBigDataGov, ControlParent, huemulType_Frequency.MONTHLY)    
    
    try {             
      /*************** AGREGAR PARAMETROS A CONTROL **********************/
      Control.AddParamYear("param_year", param_year)
      Control.AddParamMonth("param_month", param_month)
      
      /*************** ABRE RAW DESDE DATALAKE **********************/
      Control.NewStep("Abre DataLake")  
      var DF_RAW =  new yellow_tripdata_mes(huemulBigDataGov, Control)
      if (!DF_RAW.open("DF_RAW", Control, param_year, param_month, 1, 0, 0, 0))       
        Control.RaiseError(s"error encontrado, abortar: ${DF_RAW.Error.ControlError_Message}")
      
      Control.NewStep("Guardando datos en bruto en sandbox")
      DF_RAW.DataFramehuemul.savePersistToDisk(true, s"taxi_yellow_${param_year}_${param_month}", "taxi") //, globalPath, databaseName)
      
      /*********************************************************/
      /*************** LOGICAS DE NEGOCIO **********************/
      /*********************************************************/
      //instancia de clase tbl_demo_taxi_mes 
      val huemulTable = new tbl_demo_taxi(huemulBigDataGov, Control)
      
      Control.NewStep("Generar Logica de Negocio")
      huemulTable.DF_from_SQL("FinalRAW"
                          , s"""SELECT TO_DATE("${param_year}-${param_month}-1") as periodo_mes
                                     ,VendorID
                                     ,tpep_pickup_datetime
                                     ,tpep_dropoff_datetime
                                     ,passenger_count
                                     ,cast(case when trip_distance is null or trip_distance = '' then 0 else trip_distance end as Decimal(6,4)) as trip_distance
                                     ,RatecodeID
                                     ,store_and_fwd_flag
                                     ,PULocationID
                                     ,DOLocationID
                                     ,payment_type
                                     ,fare_amount
                                     ,extra
                                     ,mta_tax
                                     ,tip_amount
                                     ,tolls_amount
                                     ,improvement_surcharge
                                     ,cast(case when total_amount is null or total_amount  = '' then 0 else total_amount end as Decimal(6,4)) as total_amount
                                     ,congestion_surcharge
                                     ,concat(VendorID,'-',row_number() over(partition by VendorID order by 1)) as UniqueKey 

                               FROM DF_RAW """)
      
                               huemulTable.DataFramehuemul.DataFrame.show()
      
      
      Control.NewStep("Asocia columnas de la tabla con nombres de campos de SQL")
      
      huemulTable.periodo_mes.SetMapping("periodo_mes")
      huemulTable.VendorID.SetMapping("VendorID")
      huemulTable.tpep_pickup_datetime.SetMapping("tpep_pickup_datetime")
      huemulTable.tpep_dropoff_datetime.SetMapping("tpep_dropoff_datetime")
      huemulTable.passenger_count.SetMapping("passenger_count")
      huemulTable.trip_distance.SetMapping("trip_distance")
      huemulTable.RatecodeID.SetMapping("RatecodeID")
      huemulTable.store_and_fwd_flag.SetMapping("store_and_fwd_flag")
      huemulTable.PULocationID.SetMapping("PULocationID")
      huemulTable.DOLocationID.SetMapping("DOLocationID")
      huemulTable.payment_type.SetMapping("payment_type")
      huemulTable.fare_amount.SetMapping("fare_amount")
      huemulTable.extra.SetMapping("extra")
      huemulTable.mta_tax.SetMapping("mta_tax")
      huemulTable.tip_amount.SetMapping("tip_amount")
      huemulTable.tolls_amount.SetMapping("tolls_amount")
      huemulTable.improvement_surcharge.SetMapping("improvement_surcharge")
      huemulTable.total_amount.SetMapping("total_amount")
      huemulTable.congestion_surcharge.SetMapping("congestion_surcharge")
      huemulTable.UniqueKey.SetMapping("UniqueKey")

      
      
      // huemulTable.setApplyDistinct(false) //deshabilitar si DF tiene datos únicos, (está habilitado por default)

      Control.NewStep("Ejecuta Proceso")    
      if (!huemulTable.executeFull("FinalSaved"))
        Control.RaiseError(s"User: Error al intentar masterizar taxis yellow ny (${huemulTable.Error_Code}): ${huemulTable.Error_Text}")
      
      Control.FinishProcessOK
    } catch {
      case e: Exception => {
        Control.Control_Error.GetError(e, this.getClass.getName, null)
        Control.FinishProcessError()
      }
    }
    
    return Control   
  }
  
}
