package generador

import org.junit._
import Assert._
import com.yourcompany.demo_taxi.datalake._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.huemulType_Tables

@Test
class AppTest {
    val args: Array[String] = new Array[String](1)
    args(0) = "Environment=production,RegisterInControl=false,TestPlanMode=true"
      
    val huemulBigDataGov = new huemul_BigDataGovernance("Generador de Codigo",args,globalSettings.Global)
    val Control = new huemul_Control(huemulBigDataGov,null,  huemulType_Frequency.ANY_MOMENT)

    @Test
    def testOK() = assertTrue(GeneraCod)

    def GeneraCod(): Boolean = {            
      var row_class = new yellow_tripdata_mes(huemulBigDataGov,Control)
      row_class.GenerateInitialCode("com.yourcompany" //PackageBase
                                  , "demo_taxi"//PackageProject
                                  , "process_taxi_mes"//NewObjectName
                                  , "tbl_demo_taxi_mes"//NewTableName
                                  
                                  , huemulType_Tables.Transaction //TableType
                                  ,  huemulType_Frequency.MONTHLY //EsMes
                                  , false //AutoMapping)
                                  )                    
       return true
    }
    
}


