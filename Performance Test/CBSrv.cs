using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Query;

namespace Performance_Test
{
    class CBSrv
    {
        private ICluster cbCluster;
        private IBucket cbBucket;
        private ICouchbaseCollection cbCollection;
        private string apiType;
        private int numberOfPopulatingThreads;
        private int numberOfWritingThreads  = 0;
        private int numberOfReadingThreads  = 0;
        private int datasetSize = 0;
        private int numberOfOperations = 0;
        private string insertSQLText = "";
        private string updateSQLText = "";
        private string querySQLText = "";
        private bool showError = false;

        public CBSrv(Dictionary<string, string> TestConfig)
        {
            cbCluster = Cluster.ConnectAsync("couchbase://" + TestConfig["ClusterAddress"], TestConfig["UserName"], TestConfig["Password"]).Result;
            cbBucket = cbCluster.BucketAsync(TestConfig["DatabaseName"]).Result;
            cbCollection = cbBucket.DefaultCollection();

            apiType = TestConfig["Target"];
            numberOfPopulatingThreads = Int32.Parse(TestConfig["NumberOfPopulatingThreads"]);
            numberOfWritingThreads  = Int32.Parse(TestConfig["NumberOfWritingThreads"]);
            numberOfReadingThreads  = Int32.Parse(TestConfig["NumberOfReadingThreads"]);
            datasetSize = Int32.Parse(TestConfig["DatasetSize"]);
            numberOfOperations = Int32.Parse(TestConfig["NumberOfOperations"]);
            
            showError = Int32.Parse(TestConfig["ShowError"]) == 0 ? false : true;
            
            insertSQLText = "INSERT INTO " + TestConfig["DatabaseName"] + " (KEY, VALUE) VALUES "
                            + " ($docKey, $docValue)";
            updateSQLText = "UPDATE " + TestConfig["DatabaseName"] + " USE KEYS $orderID SET outlet=$outlet, model=$model, color=$color, " +
                            " unitPrice=$unitPrice, quantity=$quantity, lastUpdate=$lastUpdate";
            querySQLText = "SELECT * FROM " + TestConfig["DatabaseName"] + " USE KEYS $orderID";
        }

        public void Start()
        {
            Stopwatch swMain = new Stopwatch();
            
            ResetDatabase();

            swMain.Start();

            Thread[] populatingThreads = new Thread[numberOfPopulatingThreads];
            for (int n = 0; n < numberOfPopulatingThreads; n++) 
            {
                int startNumber = n * datasetSize / numberOfPopulatingThreads + 1;
                int endNumber = (n + 1) * datasetSize / numberOfPopulatingThreads;
                endNumber = endNumber >= datasetSize ? datasetSize : endNumber;
                if (apiType.Equals("cbkv", StringComparison.OrdinalIgnoreCase)) 
                {
                    populatingThreads[n] = new Thread(()=>KVPopulate(startNumber, endNumber));
                    populatingThreads[n].Start();
                }
                else if (apiType.Equals("cbquery", StringComparison.OrdinalIgnoreCase)) 
                {
                    populatingThreads[n] = new Thread(()=>QueryPopulate(startNumber, endNumber));
                    populatingThreads[n].Start();
                }
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

            swMain.Stop();
            Console.WriteLine("Test bucket was prepared in {0:00}:{1:00}:{2:00}.", swMain.Elapsed.Hours, swMain.Elapsed.Minutes, swMain.Elapsed.Seconds);

            swMain.Reset();            
            swMain.Start();

            Thread[] writingThreads = new Thread[numberOfWritingThreads];
            Thread[] readingThreads = new Thread[numberOfReadingThreads];

            for (int n = 0; n < numberOfWritingThreads; n++) 
            {
                if (apiType.Equals("cbkv", StringComparison.OrdinalIgnoreCase)) 
                {
                    writingThreads[n] = new Thread(()=>KVUpdate());
                    writingThreads[n].Start();
                }
                else if (apiType.Equals("cbquery", StringComparison.OrdinalIgnoreCase)) 
                {
                    writingThreads[n] = new Thread(()=>QueryUpdate());
                    writingThreads[n].Start();
                }
            }

            for (int n = 0; n < numberOfReadingThreads; n++) 
            {
                if (apiType.Equals("cbkv", StringComparison.OrdinalIgnoreCase))
                {
                    readingThreads[n] = new Thread(()=>KVGet());
                    readingThreads[n].Start();
                }
                else if (apiType.Equals("cbquery", StringComparison.OrdinalIgnoreCase)) 
                {
                    readingThreads[n] = new Thread(()=>QueryGet());
                    readingThreads[n].Start();
                }
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

            swMain.Stop();
            Console.WriteLine("Testing was finished in {0:00}:{1:00}:{2:00}.", swMain.Elapsed.Hours, swMain.Elapsed.Minutes, swMain.Elapsed.Seconds);

            PrintResult().Wait();
            return;
        }

        private void ResetDatabase()
        {
            try
            {
                Task taskFlush = cbCluster.Buckets.FlushBucketAsync(cbBucket.Name);
                Task.WaitAll(taskFlush);
            }
            catch (Exception e)
            {
                if (showError) Console.WriteLine("Exception was captured in resetting step. {0}", e.Message);
                // Console.WriteLine(e.StackTrace);
            }
        }

        private void KVPopulate(int StartNumber, int EndNumber) 
        {
            int retry  = 0;

            for(int n = StartNumber; n <= EndNumber; n++)
            {
                var tsString = DateTime.Now.ToString("s");
                string randomOrderID = "Order_" + n.ToString();
                string randomName = Payload.GetName();
                string randomGender = Payload.GetGender();
                int randomAge = Payload.GetAge();
                string randomOutlet = Payload.GetOutlet();
                string randomModel = Payload.GetModel();
                string randomColor = Payload.GetColor();
                double randomUnitPrice = Payload.GetUnitPrice();
                int randomQuantity = Payload.GetQuantity();
                var document = new { docType = "Order", name = randomName, gender = randomGender, age = randomAge, 
                                    outlet = randomOutlet, model = randomModel, color = randomColor, 
                                    unitPrice = randomUnitPrice, quantity = randomQuantity, txTime = tsString, 
                                    lastUpdate = tsString };
                try
                {
                    var result = cbCollection.InsertAsync(randomOrderID, document, options => {options.Timeout(TimeSpan.FromSeconds(60));}).Result;
                }
                catch (Exception e)
                {
                    if (showError) Console.WriteLine("Exception was captured in populating task {0}.", randomOrderID, e.Message);
                    // Console.WriteLine(e.StackTrace);
                    n--;
                    if (retry++ > 100) break;
                    continue;
                }
            }
        }

        private void KVUpdate() 
        {         
            Stopwatch sw = new Stopwatch();
            int retry = 0;

            for(int n = 0; n < numberOfOperations; n++)
            {
                string tsString = DateTime.Now.ToString("s");
                string randomOrderID = "Order_" + Payload.GetOrderID(1, datasetSize).ToString();
                string randomOutlet = Payload.GetOutlet();
                string randomModel = Payload.GetModel();
                string randomColor = Payload.GetColor();
                double randomUnitPrice = Payload.GetUnitPrice();
                int randomQuantity = Payload.GetQuantity();

                sw.Start();
                try
                {
                    var result = cbCollection.MutateInAsync(randomOrderID, specs => {
                                                                specs.Replace("outlet", randomOutlet);
                                                                specs.Replace("model", randomModel);
                                                                specs.Replace("color", randomColor);
                                                                specs.Replace("unitPrice", randomUnitPrice);
                                                                specs.Replace("quantity", randomQuantity);
                                                                specs.Replace("lastUpdate", tsString);
                                                            }, 
                                                            options => {options.Timeout(TimeSpan.FromSeconds(5));}).Result;
                    sw.Stop();
                    var latency = sw.Elapsed.TotalMilliseconds;
                    sw.Reset();

                    string gID = Guid.NewGuid().ToString();
                    var statsDoc = new { docType = "Stats", opType = "KVUpdate",  response = latency, lastUpdate = tsString};
                    var statsResult = cbCollection.InsertAsync(gID, statsDoc).Result;
                }
                catch (Exception e)
                {
                    if (showError) Console.WriteLine("Exception was captured in writing tasks. {0} {1}", randomOrderID, e.Message);
                    // Console.WriteLine(e.StackTrace);
                    n--;
                    if (retry++ > 100) break;
                    continue;
                }
            }
        }

        private void KVGet() 
        {
            Stopwatch sw = new Stopwatch();
            int retry = 0;
            
            for(int n = 0; n < numberOfOperations; n++)
            {               
                string tsString = DateTime.Now.ToString("s");
                string randomOrderID = "Order_" + Payload.GetOrderID(1, datasetSize).ToString();
                sw.Start();
                try 
                {    
                    var result = cbCollection.GetAsync(randomOrderID).Result;
                    sw.Stop();
                    var latency = sw.Elapsed.TotalMilliseconds;
                    sw.Reset();

                    string gID = Guid.NewGuid().ToString();
                    var statsDoc = new { docType = "Stats", opType = "KVGet",  response = latency, lastUpdate = tsString};
                    var statsResult = cbCollection.InsertAsync(gID, statsDoc).Result;
                } 
                catch (Exception e) {
                    if (showError) Console.WriteLine("Exception was captured in reading tasks. {0} {1}", randomOrderID, e.Message);
                    // Console.WriteLine(e.StackTrace);
                    n--;
                    if (retry++ > 100) break;
                    continue;
                }
            }
        }

        private void QueryPopulate(int StartNumber, int EndNumber) 
        {
            int retry  = 0;

            for(int n = StartNumber; n <= EndNumber; n++)
            {
                var tsString = DateTime.Now.ToString("s");
                string randomOrderID = "Order_" + n.ToString();
                string randomName = Payload.GetName();
                string randomGender = Payload.GetGender();
                int randomAge = Payload.GetAge();
                string randomOutlet = Payload.GetOutlet();
                string randomModel = Payload.GetModel();
                string randomColor = Payload.GetColor();
                double randomUnitPrice = Payload.GetUnitPrice();
                int randomQuantity = Payload.GetQuantity();
                var document = new { docType = "Order", name = randomName, gender = randomGender, age = randomAge, 
                                    outlet = randomOutlet, model = randomModel, color = randomColor, 
                                    unitPrice = randomUnitPrice, quantity = randomQuantity, txTime = tsString, 
                                    lastUpdate = tsString };
                
                try {
                    var result = cbCluster.QueryAsync<dynamic>(insertSQLText, new QueryOptions()
                                                                            .Parameter("docKey", randomOrderID)
                                                                            .Parameter("docValue", document)
                                                                            .AdHoc(false)).Result;
                } 
                catch (Exception e) 
                {
                    if (showError) Console.WriteLine("Exception was captured in populating tasks. {0} {1}", randomOrderID, e.Message);
                    // Console.WriteLine(e.StackTrace);
                    n--;
                    if (retry++ > 100) break;
                    continue;
                }
            }
        }

        private void QueryUpdate() 
        {
            Stopwatch sw = new Stopwatch();
            int retry = 0;
            
            for(int n = 0; n < numberOfOperations; n++)
            {
                string randomOrderID = "Order_" + Payload.GetOrderID(1, datasetSize).ToString();
                string randomOutlet = Payload.GetOutlet();
                string randomModel = Payload.GetModel();
                string randomColor = Payload.GetColor();
                double randomUnitPrice = Payload.GetUnitPrice();
                int randomQuantity = Payload.GetQuantity();
                string tsString = DateTime.Now.ToString("s");
                
                sw.Start();
                try
                {
                    var result = cbCluster.QueryAsync<dynamic>(updateSQLText, new QueryOptions()
                                                                            .Parameter("outlet", randomOutlet)
                                                                            .Parameter("model", randomModel)
                                                                            .Parameter("color", randomColor)
                                                                            .Parameter("unitPrice", randomUnitPrice)
                                                                            .Parameter("quantity", randomQuantity)
                                                                            .Parameter("lastUpdate", tsString)
                                                                            .Parameter("orderID", randomOrderID)
                                                                            .AdHoc(false)).Result;
                    sw.Stop();
                    var latency = sw.Elapsed.TotalMilliseconds;
                    sw.Reset();

                    string gID = Guid.NewGuid().ToString();
                    var statsDoc = new { docType = "Stats", opType = "QueryUpdate",  response = latency, lastUpdate = tsString};
                    var statsResult = cbCollection.InsertAsync(gID, statsDoc).Result;
                }
                catch (Exception e)
                {
                    if (showError) Console.WriteLine("Exception was captured in writing tasks: {0} {1}", randomOrderID, e.Message);
                    // Console.WriteLine(e.StackTrace);
                    n--;
                    if (retry++ > 100) break;
                    continue;
                }
            }
        }

        public void QueryGet() 
        {
            Stopwatch sw = new Stopwatch();
            int retry = 0;

            for(int n = 0; n < numberOfOperations; n++)
            {
                string randomOrderID = "Order_" + Payload.GetOrderID(1, datasetSize).ToString();
                var tsString = DateTime.Now.ToString("s");

                sw.Start();

                try {
                    var result = cbCluster.QueryAsync<dynamic>(querySQLText, new QueryOptions()
                                                                                 .Parameter("orderID", randomOrderID)
                                                                                 .AdHoc(false)
                                                                                 .Readonly(true)).Result;
                    sw.Stop();
                    var latency = sw.Elapsed.TotalMilliseconds;
                    sw.Reset();

                    string gID = Guid.NewGuid().ToString();
                    var statsDoc = new { docType = "Stats", opType = "QueryGet",  response = latency, lastUpdate = tsString};
                    var statsResult = cbCollection.InsertAsync(gID, statsDoc).Result;
                } 
                catch (Exception e) 
                {
                    if (showError) Console.WriteLine("Exception was captured in reading tasks. {0} {1}", randomOrderID, e.Message);
                    // Console.WriteLine(e.StackTrace);
                    n--;
                    if (retry++ > 100) break;
                    continue;
                }
            }
        }
    
        private async Task PrintResult()
        {
            try
            {
                var result = await cbCluster.AnalyticsQueryAsync<dynamic>("SELECT opType, AVG(response) AS AvgResponse, COUNT(response) AS TotalOperations FROM PerfStats GROUP BY opType;");

                await foreach (var row in result.Rows)
                {
                    Console.WriteLine("OpType: {0}, AvgResponseTime: {1}, TotalOperations: {2}", 
                                        row.opType, row.AvgResponse, row.TotalOperations);
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
