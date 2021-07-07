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
        private static Random rand = new Random();
        private static Object randLock = new Object();
        private static string resetSQLText = "TRUNCATE TABLE Orders; TRUNCATE TABLE PerfStats;";
        private static string popSQLText = "INSERT INTO Orders (OrderID, [Name], Gender, Age, Outlet, Model, Color, UnitPrice, Quantity, TxTime, LastUpdate) " + 
                                        "   VALUES (@orderID, @name, @gender, @age, @outlet, @model, @color, @unitPrice, @quantity, getdate(), getdate()); ";
        private static string updateSQLText = "UPDATE Orders SET Orders.Outlet = @outlet, Orders.Model = @model, Orders.Color = @color, " + 
                                            " Orders.UnitPrice = @unitPrice, Orders.Quantity = @quantity, Orders.LastUpdate = getdate() " + 
                                            " WHERE OrderID = @orderID; ";
        private static string querySQLText = "SELECT * FROM Orders WHERE OrderID = @OrderID";
        private static string statsSQLText = "INSERT INTO PerfStats (OpType, ResponseTime, LastUpdate) VALUES (@opType, @responseTime, sysdatetime());";
        private int numberOfPopulatingThreads = 0;
        private int numberOfWritingThreads  = 0;
        private int numberOfReadingThreads  = 0;
        private int datasetSize = 0;
        private int numberOfOperations = 0;
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

            sqlConn = new SqlConnection(sqlConnStrBuilder.ConnectionString);
            try{
                sqlConn.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        
        public void Start()
        {
            Stopwatch swMain = new Stopwatch();
            
            Console.WriteLine("Reset database...");
            Reset();

            Console.WriteLine("Start populating database...");
            swMain.Start();

            Task[] populatingTasks = new Task[numberOfPopulatingThreads];
            for (int n = 0; n < numberOfPopulatingThreads; n++) 
            {
                int startNumber = n * datasetSize / numberOfPopulatingThreads + 1;
                int endNumber = (n + 1) * datasetSize / numberOfPopulatingThreads;
                endNumber = endNumber >= datasetSize ? datasetSize : endNumber;
                populatingTasks[n] = Task.Factory.StartNew(()=>Populate(startNumber, endNumber));
            }

            Task.WaitAll(populatingTasks);
            swMain.Stop();
            Console.WriteLine("Test dataset was populated in {0:00}:{1:00}:{2:00}. Start testing...", swMain.Elapsed.Hours, swMain.Elapsed.Minutes, swMain.Elapsed.Seconds);

            swMain.Reset();
            Task[] testingTasks = new Task[numberOfReadingThreads + numberOfWritingThreads];
            swMain.Start();
            
            for (int n = 0; n < numberOfWritingThreads; n++) 
            {
                int startNumber = n * datasetSize / numberOfWritingThreads + 1;
                int endNumber = (n + 1) * datasetSize / numberOfWritingThreads;
                endNumber = endNumber >= datasetSize ? datasetSize : endNumber;
                testingTasks[n] = Task.Factory.StartNew(()=>Update(n, startNumber, endNumber));
            }

            for (int n = 0; n < numberOfReadingThreads; n++) 
            {
                testingTasks[numberOfWritingThreads + n] = Task.Factory.StartNew(()=>Get());
            }
            
            Task.WaitAll(testingTasks);
            swMain.Stop();
            Console.WriteLine("Testing was finished in {0:00}:{1:00}:{2:00}. Start testing...", swMain.Elapsed.Hours, swMain.Elapsed.Minutes, swMain.Elapsed.Seconds);
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
                Console.WriteLine("Exception was captured in resetting step.");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
        }

        private void Populate(int StartNumber, int EndNumber) 
        {
            try
            {
                using (SqlConnection sqlConnPop = new SqlConnection(sqlConnStrBuilder.ConnectionString))
                {
                    sqlConnPop.Open();
                    for(int n = StartNumber; n <= EndNumber; n++) 
                    {
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
                                sqlCmdUpsert.Parameters["@name"].Value = Payload.GetName();
                                sqlCmdUpsert.Parameters["@gender"].Value = Payload.GetGender();
                                sqlCmdUpsert.Parameters["@age"].Value = Payload.GetAge();
                                sqlCmdUpsert.Parameters["@outlet"].Value = Payload.GetOutlet();
                                sqlCmdUpsert.Parameters["@model"].Value = Payload.GetModel();
                                sqlCmdUpsert.Parameters["@color"].Value = Payload.GetColor();
                                sqlCmdUpsert.Parameters["@unitPrice"].Value = Payload.GetUnitPrice();
                                sqlCmdUpsert.Parameters["@quantity"].Value = Payload.GetQuantity();

                                sqlCmdUpsert.ExecuteNonQuery();
                            }
                        }
                        catch (SqlException e) 
                        {
                            Console.WriteLine("Exception was captured in populating step.");
                            Console.WriteLine(e.Message);
                            Console.WriteLine(e.StackTrace);
                            continue;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception was captured in dataset populating step.");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
        }

        private void Update(int ThreadNumber, int StartNumber, int EndNumber) 
        {            
            Stopwatch sw = new Stopwatch();
            double latency = 0;
            int n = 0;

            try
            {
                using (SqlConnection sqlConnUpdate = new SqlConnection(sqlConnStrBuilder.ConnectionString))
                {
                    sqlConnUpdate.Open();
                    while(n++ < numberOfOperations) 
                    {
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

                                sqlCmdUpdate.Parameters["@orderID"].Value = Payload.GetOrderID(1, datasetSize + 1);
                                sqlCmdUpdate.Parameters["@outlet"].Value = Payload.GetOutlet();
                                sqlCmdUpdate.Parameters["@model"].Value = Payload.GetModel();
                                sqlCmdUpdate.Parameters["@color"].Value = Payload.GetColor();
                                sqlCmdUpdate.Parameters["@unitPrice"].Value = Payload.GetUnitPrice();
                                sqlCmdUpdate.Parameters["@quantity"].Value = Payload.GetQuantity();

                                sw.Start();   
                                int result = sqlCmdUpdate.ExecuteNonQuery();
                                sw.Stop();
                                latency = sw.ElapsedMilliseconds;
                                sw.Reset();
                                if (result < 1) continue;
                            }
                        
                            // Console.WriteLine("Write response time: {0} ms", latency);

                            using (SqlCommand sqlCmdStats = new SqlCommand(statsSQLText, sqlConnUpdate)) {
                                sqlCmdStats.Parameters.Add("@opType", SqlDbType.NVarChar);
                                sqlCmdStats.Parameters.Add("@responseTime", SqlDbType.Float);

                                sqlCmdStats.Parameters["@opType"].Value = "Update";
                                sqlCmdStats.Parameters["@responseTime"].Value = latency;

                                int result = sqlCmdStats.ExecuteNonQuery();
                            }
                        }
                        catch (SqlException e) 
                        {
                            Console.WriteLine("Exception was captured in writing tasks.");
                            Console.WriteLine(e.Message);
                            Console.WriteLine(e.StackTrace);
                            continue;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception was captured in writing tasks.");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
        
        }

        private void Get() 
        {
            Stopwatch sw = new Stopwatch();
            double latency = 0;
            int n = 0;
            
            try
            {
                using (SqlConnection sqlConnQuery = new SqlConnection(sqlConnStrBuilder.ConnectionString))
                {
                    sqlConnQuery.Open();
                    while(n++ < numberOfOperations) 
                    {
                        try 
                        {
                            using (SqlCommand sqlCmdQuery = new SqlCommand(querySQLText, sqlConnQuery))
                            {
                                sqlCmdQuery.Parameters.Add("@orderID", SqlDbType.Int);
                                
                                int orderID = Payload.GetOrderID(1, datasetSize);
                                sqlCmdQuery.Parameters["@orderID"].Value = orderID;

                                sw.Start();
                                using (SqlDataReader sqlReaderQuery = sqlCmdQuery.ExecuteReader()) 
                                {
                                    sw.Stop();
                                    latency = sw.ElapsedMilliseconds;
                                    sw.Reset();
                                    if (! sqlReaderQuery.HasRows) continue;
                                }
                            }
                            
                            // Console.WriteLine("Read response time: {0} ms", latency);

                            using (SqlCommand sqlCmdStats = new SqlCommand(statsSQLText, sqlConnQuery)) 
                            {
                                sqlCmdStats.Parameters.Add("@opType", SqlDbType.NVarChar);
                                sqlCmdStats.Parameters.Add("@responseTime", SqlDbType.Float);

                                sqlCmdStats.Parameters["@opType"].Value = "Query";
                                sqlCmdStats.Parameters["@responseTime"].Value = latency;

                                int result = sqlCmdStats.ExecuteNonQuery();
                            }
                        }
                        catch (SqlException e) {
                            Console.WriteLine("Exception was captured in reading tasks.");
                            Console.WriteLine(e.Message);
                            continue;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception was captured in reading tasks.");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
        }
    }
}
