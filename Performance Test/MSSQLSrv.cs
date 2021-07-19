using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO;

namespace Performance_Test
{
    class MSSQLSrv
    {
        private static string resetSQLText = "TRUNCATE TABLE Orders; TRUNCATE TABLE PerfStats;";
        private static string popSQLText = "INSERT INTO Orders (OrderID, [Name], Gender, Age, Outlet, Model, Color, UnitPrice, Quantity, TxTime, LastUpdate) " + 
                                        "   VALUES (@orderID, @name, @gender, @age, @outlet, @model, @color, @unitPrice, @quantity, getdate(), getdate()); ";
        private static string updateSQLText = "UPDATE Orders SET Orders.Outlet = @outlet, Orders.Model = @model, Orders.Color = @color, " + 
                                            " Orders.UnitPrice = @unitPrice, Orders.Quantity = @quantity, Orders.LastUpdate = getdate() " + 
                                            " WHERE OrderID = @orderID; ";
        private static string querySQLText = "SELECT * FROM Orders WHERE OrderID = @OrderID";
        private static string statsSQLText = "INSERT INTO PerfStats (OrderID, OpType, ResponseTime, LastUpdate) VALUES (@OrderID, @opType, @responseTime, sysdatetime());";
        private static string resultSQLText = "SELECT OpType, Avg(ResponseTime) AS AvgResponse, COUNT(ResponseTime) AS TotalOperations FROM PerfStats GROUP BY OpType";
        private int numberOfPopulatingThreads = 0;
        private int numberOfWritingThreads  = 0;
        private int numberOfReadingThreads  = 0;
        private int datasetSize = 0;
        private int numberOfOperations = 0;
        private bool showError = false;
        private SqlConnectionStringBuilder sqlConnStrBuilder;
        private static SqlConnection sqlConn;

        public MSSQLSrv(Dictionary<string, string> TestConfig)
        {
            numberOfPopulatingThreads = Int32.Parse(TestConfig["NumberOfPopulatingThreads"]);
            numberOfWritingThreads  = Int32.Parse(TestConfig["NumberOfWritingThreads"]);
            numberOfReadingThreads  = Int32.Parse(TestConfig["NumberOfReadingThreads"]);
            datasetSize = Int32.Parse(TestConfig["DatasetSize"]);
            numberOfOperations = Int32.Parse(TestConfig["NumberOfOperations"]);

            sqlConnStrBuilder = new SqlConnectionStringBuilder();
            sqlConnStrBuilder.DataSource = TestConfig["ClusterAddress"]; 
            sqlConnStrBuilder.UserID = TestConfig["UserName"];            
            sqlConnStrBuilder.Password = TestConfig["Password"];     
            sqlConnStrBuilder.InitialCatalog = TestConfig["DatabaseName"];
            sqlConnStrBuilder.Pooling = true;
            sqlConnStrBuilder.MaxPoolSize = numberOfReadingThreads + numberOfWritingThreads + 10;
            sqlConnStrBuilder.MinPoolSize = numberOfReadingThreads + numberOfWritingThreads;

            showError = showError = Int32.Parse(TestConfig["ShowError"]) == 0 ? false : true;

            sqlConn = new SqlConnection(sqlConnStrBuilder.ConnectionString);
            try{
                sqlConn.Open();
            }
            catch (Exception e)
            {
                if (showError) Console.WriteLine("Exception was captured in initialization step. {0}", e.Message);
                // Console.WriteLine(e.StackTrace);
                throw(new Exception("Unable to open connection to SQL Server"));
            }
        }
        
        public void Start()
        {
            Stopwatch swMain = new Stopwatch();
            
            Reset();

            swMain.Start();

            Thread[] populatingThreads = new Thread[numberOfPopulatingThreads];
            for (int n = 0; n < numberOfPopulatingThreads; n++) 
            {
                int startNumber = n * datasetSize / numberOfPopulatingThreads + 1;
                int endNumber = (n + 1) * datasetSize / numberOfPopulatingThreads;
                endNumber = endNumber >= datasetSize ? datasetSize : endNumber;
                populatingThreads[n] = new Thread(()=>Populate(startNumber, endNumber));
                populatingThreads[n].Start();
            }

            bool populatingStatus = true;
            while (populatingStatus)
            {
                int runningThread = 0;
                Thread.Sleep(500);
                for (int n = 0; n < numberOfPopulatingThreads; n++)
                {
                    runningThread += populatingThreads[n].IsAlive ? 1 : 0;
                }

                if (runningThread == 0) populatingStatus = false;
            }

            // Task[] populatingTasks = new Task[numberOfPopulatingThreads];
            // for (int n = 0; n < numberOfPopulatingThreads; n++) 
            // {
            //     int startNumber = n * datasetSize / numberOfPopulatingThreads + 1;
            //     int endNumber = (n + 1) * datasetSize / numberOfPopulatingThreads;
            //     endNumber = endNumber >= datasetSize ? datasetSize : endNumber;
            //     populatingTasks[n] = Task.Factory.StartNew(()=>Populate(startNumber, endNumber));
            // }

            // Task.WaitAll(populatingTasks);
            swMain.Stop();
            Console.WriteLine("Test database was prepared in {0:00}:{1:00}:{2:00}.", swMain.Elapsed.Hours, swMain.Elapsed.Minutes, swMain.Elapsed.Seconds);

            swMain.Reset();
            swMain.Start();

            Thread[] writingThreads = new Thread[numberOfWritingThreads];
            Thread[] readingThreads = new Thread[numberOfReadingThreads];
            
                           
            for (int n = 0; n < numberOfWritingThreads; n++) 
            {
                writingThreads[n] = new Thread(()=>Update());
                writingThreads[n].Start();
            }
            
            for (int n = 0; n < numberOfReadingThreads; n++) 
            {
                readingThreads[n] = new Thread(()=>Get());
                readingThreads[n].Start();
            }
             
            bool testingStatus = true;
            while (testingStatus)
            {
                int runningThread = 0;
                Thread.Sleep(1000);
                for (int n = 0; n < numberOfWritingThreads; n++)
                {
                    runningThread += writingThreads[n].IsAlive ? 1 : 0;
                }

                for (int n = 0; n < numberOfReadingThreads; n++)
                {
                    runningThread += readingThreads[n].IsAlive ? 1 : 0;
                }

                if (runningThread == 0) testingStatus = false;
            }

            // Task[] testingTasks = new Task[numberOfReadingThreads + numberOfWritingThreads];

            // for (int n = 0; n < numberOfWritingThreads; n++) 
            // {
            //     int startNumber = n * datasetSize / numberOfWritingThreads + 1;
            //     int endNumber = (n + 1) * datasetSize / numberOfWritingThreads;
            //     endNumber = endNumber >= datasetSize ? datasetSize : endNumber;
            //     testingTasks[n] = Task.Factory.StartNew(()=>Update());
            // }

            // for (int n = 0; n < numberOfReadingThreads; n++) 
            // {
            //     testingTasks[numberOfWritingThreads + n] = Task.Factory.StartNew(()=>Get());
            // }
            
            // Task.WaitAll(testingTasks);
            swMain.Stop();
            Console.WriteLine("Testing was finished in {0:00}:{1:00}:{2:00}.", swMain.Elapsed.Hours, swMain.Elapsed.Minutes, swMain.Elapsed.Seconds);

            PrintResult();

            return;
        }

        private void Reset()
        {
            try
            {
                using (SqlConnection sqlConnReset = new SqlConnection(sqlConnStrBuilder.ConnectionString))
                {
                    sqlConnReset.Open();
                    using (SqlCommand sqlCmdReset = new SqlCommand(resetSQLText, sqlConnReset))
                    {
                        sqlCmdReset.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception e)
            {
                if (showError) Console.WriteLine("Exception was captured in resetting step. {0}", e.Message);
                // Console.WriteLine(e.StackTrace);
            }
        }

        private void Populate(int StartNumber, int EndNumber) 
        {
            int retry = 0;

            try
            {
                using (SqlConnection sqlConnPop = new SqlConnection(sqlConnStrBuilder.ConnectionString))
                {
                    sqlConnPop.Open();

                    for(int n = StartNumber; n <= EndNumber; n++) 
                    {
                        string randomName = Payload.GetName();
                        string randomGender = Payload.GetGender();
                        int randomAge = Payload.GetAge();
                        string randomOutlet = Payload.GetOutlet();
                        string randomModel = Payload.GetModel();
                        string randomColor = Payload.GetColor();
                        double randomUnitPrice = Payload.GetUnitPrice();
                        int randomQuantity = Payload.GetQuantity();

                        try 
                        {
                            using (SqlCommand sqlCmdUpsert = new SqlCommand(popSQLText, sqlConnPop))
                            {
                                sqlCmdUpsert.Parameters.Add("@orderID", SqlDbType.Int);
                                sqlCmdUpsert.Parameters.Add("@name", SqlDbType.NVarChar);
                                sqlCmdUpsert.Parameters.Add("@gender", SqlDbType.NVarChar);
                                sqlCmdUpsert.Parameters.Add("@age", SqlDbType.TinyInt);
                                sqlCmdUpsert.Parameters.Add("@outlet", SqlDbType.NVarChar);
                                sqlCmdUpsert.Parameters.Add("@model", SqlDbType.NVarChar);
                                sqlCmdUpsert.Parameters.Add("@color", SqlDbType.NVarChar);
                                sqlCmdUpsert.Parameters.Add("@unitPrice", SqlDbType.Decimal);
                                sqlCmdUpsert.Parameters.Add("@quantity", SqlDbType.Int);

                                sqlCmdUpsert.Parameters["@orderID"].Value = n;
                                sqlCmdUpsert.Parameters["@name"].Value = randomName;
                                sqlCmdUpsert.Parameters["@gender"].Value = randomGender;
                                sqlCmdUpsert.Parameters["@age"].Value = randomAge;
                                sqlCmdUpsert.Parameters["@outlet"].Value = randomOutlet;
                                sqlCmdUpsert.Parameters["@model"].Value = randomModel;
                                sqlCmdUpsert.Parameters["@color"].Value = randomColor;
                                sqlCmdUpsert.Parameters["@unitPrice"].Value = randomUnitPrice;
                                sqlCmdUpsert.Parameters["@quantity"].Value = randomQuantity;

                                sqlCmdUpsert.ExecuteNonQuery();
                            }
                        }
                        catch (SqlException e) 
                        {
                            if (showError) Console.WriteLine("Exception was captured in populating step. {0} {1}", n, e.Message);
                            // Console.WriteLine(e.StackTrace);
                            if (retry++ > 100) break;
                            continue;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (showError) Console.WriteLine("Exception was captured in dataset populating step. {0}", e.Message);
                // Console.WriteLine(e.StackTrace);
            }
        }

        private void Update() 
        {            
            Stopwatch sw = new Stopwatch();
            double latency = 0;
            int retry = 0;
            int n = 0;
    
            try
            {
                using (SqlConnection sqlConnUpdate = new SqlConnection(sqlConnStrBuilder.ConnectionString))
                {
                    sqlConnUpdate.Open();
                    while(n++ < numberOfOperations) 
                    {
                        int randomOrderID = Payload.GetOrderID(1, datasetSize);
                        string randomOutlet = Payload.GetOutlet();
                        string randomModel = Payload.GetModel();
                        string randomColor = Payload.GetColor();
                        double randomUnitPrice = Payload.GetUnitPrice();
                        int randomQuantity = Payload.GetQuantity();
                                
                        try 
                        {
                            using (SqlCommand sqlCmdUpdate = new SqlCommand(updateSQLText, sqlConnUpdate))
                            {
                                sqlCmdUpdate.Parameters.Add("@orderID", SqlDbType.Int);
                                sqlCmdUpdate.Parameters.Add("@outlet", SqlDbType.NVarChar);
                                sqlCmdUpdate.Parameters.Add("@model", SqlDbType.NVarChar);
                                sqlCmdUpdate.Parameters.Add("@color", SqlDbType.NVarChar);
                                sqlCmdUpdate.Parameters.Add("@unitPrice", SqlDbType.Decimal);
                                sqlCmdUpdate.Parameters.Add("@quantity", SqlDbType.Int);

                                sqlCmdUpdate.Parameters["@orderID"].Value = randomOrderID;
                                sqlCmdUpdate.Parameters["@outlet"].Value = randomOutlet;
                                sqlCmdUpdate.Parameters["@model"].Value = randomModel;
                                sqlCmdUpdate.Parameters["@color"].Value = randomColor;
                                sqlCmdUpdate.Parameters["@unitPrice"].Value = randomUnitPrice;
                                sqlCmdUpdate.Parameters["@quantity"].Value = randomQuantity;

                                sw.Start();   
                                int result = sqlCmdUpdate.ExecuteNonQuery();
                                sw.Stop();
                                latency = sw.Elapsed.TotalMilliseconds;
                                sw.Reset();
                                if (result < 1) continue;
                            }

                            using (SqlCommand sqlCmdStats = new SqlCommand(statsSQLText, sqlConnUpdate)) {
                                sqlCmdStats.Parameters.Add("@orderID", SqlDbType.Int);
                                sqlCmdStats.Parameters.Add("@opType", SqlDbType.NVarChar);
                                sqlCmdStats.Parameters.Add("@responseTime", SqlDbType.Float);

                                sqlCmdStats.Parameters["@orderID"].Value = randomOrderID;
                                sqlCmdStats.Parameters["@opType"].Value = "Update";
                                sqlCmdStats.Parameters["@responseTime"].Value = latency;

                                int result = sqlCmdStats.ExecuteNonQuery();
                            }
                        }
                        catch (SqlException e) 
                        {
                            Console.WriteLine("Exception was captured in writing tasks. {0} {1}", randomOrderID, e.Message);
                            // Console.WriteLine(e.StackTrace);
                            n--;
                            if (retry++ > 100) break;
                            continue;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (showError) Console.WriteLine("Exception was captured in writing tasks. {0}", e.Message);
                // Console.WriteLine(e.StackTrace);
            }
        
        }

        private void Get() 
        {
            Stopwatch sw = new Stopwatch();
            int retry = 0;
            double latency = 0;
            int n = 0;
            
            try
            {
                using (SqlConnection sqlConnQuery = new SqlConnection(sqlConnStrBuilder.ConnectionString))
                {
                    sqlConnQuery.Open();
                    while(n++ < numberOfOperations) 
                    {
                        int randomOrderID = Payload.GetOrderID(1, datasetSize);
                        try 
                        {
                            using (SqlCommand sqlCmdQuery = new SqlCommand(querySQLText, sqlConnQuery))
                            {
                                sqlCmdQuery.Parameters.Add("@orderID", SqlDbType.Int);
                                
                                sqlCmdQuery.Parameters["@orderID"].Value = randomOrderID;

                                sw.Start();
                                using (SqlDataReader sqlReaderQuery = sqlCmdQuery.ExecuteReader()) 
                                {
                                    sw.Stop();
                                    latency = sw.Elapsed.TotalMilliseconds;
                                    sw.Reset();
                                    if (! sqlReaderQuery.HasRows) continue;
                                }
                            }

                            using (SqlCommand sqlCmdStats = new SqlCommand(statsSQLText, sqlConnQuery)) 
                            {
                                sqlCmdStats.Parameters.Add("@orderID", SqlDbType.Int);
                                sqlCmdStats.Parameters.Add("@opType", SqlDbType.NVarChar);
                                sqlCmdStats.Parameters.Add("@responseTime", SqlDbType.Float);

                                sqlCmdStats.Parameters["@orderID"].Value = randomOrderID;
                                sqlCmdStats.Parameters["@opType"].Value = "Query";
                                sqlCmdStats.Parameters["@responseTime"].Value = latency;

                                int result = sqlCmdStats.ExecuteNonQuery();
                            }
                        }
                        catch (SqlException e) {
                            if (showError) Console.WriteLine("Exception was captured in reading tasks. {0} {1}", randomOrderID, e.Message);
                            // Console.WriteLine(e.StackTrace);
                            n--;
                            if (retry++ > 100) break;
                            continue;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (showError) Console.WriteLine("Exception was captured in reading tasks. {0}", e.Message);
                // Console.WriteLine(e.StackTrace);
            }
        }
    
        private void PrintResult()
        {
            try
            {
                using (SqlConnection sqlConnResult = new SqlConnection(sqlConnStrBuilder.ConnectionString))
                {
                    sqlConnResult.Open();
                    using (SqlCommand sqlCmdResult = new SqlCommand(resultSQLText, sqlConnResult))
                    {
                        using (SqlDataReader sqlReaderResult = sqlCmdResult.ExecuteReader()) 
                        {
                            while (sqlReaderResult.Read())
                            {
                                Console.WriteLine("OpType: {0}, AvgResponseTime: {1}, TotalOperations: {2}", 
                                                    sqlReaderResult[0], sqlReaderResult[1], sqlReaderResult[2]);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (showError) Console.WriteLine("Exception was captured in printing result. {0}", e.Message);
                // Console.WriteLine(e.StackTrace);
            }
        }
    }
}
