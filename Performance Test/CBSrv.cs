using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Couchbase;
using Couchbase.KeyValue;

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
        private string updateSQLText = "";
        private string querySQLText = "";

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
            
            updateSQLText = "UPDATE " + TestConfig["DatabaseName"] + " SET outlet = @outlet, model = @model, color = @color, " +
                            " unitPrice = @unitPrice, quantity = @quantity, lastUpdate = @lastUpdate WHERE meta().id = @orderID";
            querySQLText = "SELECT * FROM " + TestConfig["DatabaseName"] + " WHERE meta().id = @orderID";
        }

        public void Start()
        {
            Stopwatch swMain = new Stopwatch();
            
            Console.WriteLine("Reset database...");
            ResetDatabase();

            Console.WriteLine("Start populating database...");
            swMain.Start();

            Thread[] populatingThreads = new Thread[numberOfPopulatingThreads];
            for (int n = 0; n < numberOfPopulatingThreads; n++) 
            {
                int startNumber = n * datasetSize / numberOfPopulatingThreads + 1;
                int endNumber = (n + 1) * datasetSize / numberOfPopulatingThreads;
                endNumber = endNumber >= datasetSize ? datasetSize : endNumber;
                populatingThreads[n] = new Thread(()=>KVPopulate(n, startNumber, endNumber));
                populatingThreads[n].Start();
            }

            Console.WriteLine("Waiting for populating");
            bool populatingStatus = true;
            while (populatingStatus)
            {
                int runningThread = 0;
                Thread.Sleep(500);
                for (int n = 0; n < numberOfWritingThreads; n++)
                {
                    runningThread += populatingThreads[n].IsAlive ? 1 : 0;
                }

                if (runningThread == 0) populatingStatus = false;
            }

            swMain.Stop();
            Console.WriteLine("Test dataset was populated in {0:00}:{1:00}:{2:00}. Start testing...", swMain.Elapsed.Hours, swMain.Elapsed.Minutes, swMain.Elapsed.Seconds);

            swMain.Reset();
            
            swMain.Start();
            Thread[] writingThreads = new Thread[numberOfWritingThreads];
            Thread[] readingThreads = new Thread[numberOfReadingThreads];

            if (apiType.Equals("cbkv", StringComparison.OrdinalIgnoreCase)) 
            {                
                for (int n = 0; n < numberOfWritingThreads; n++) 
                {
                    writingThreads[n] = new Thread(()=>KVUpdate());
                    writingThreads[n].Start();
                }
                
                for (int n = 0; n < numberOfReadingThreads; n++) 
                {
                    readingThreads[n] = new Thread(()=>KVGet());
                    readingThreads[n].Start();
                }
            } 
            else if (apiType.Equals("cbquery", StringComparison.OrdinalIgnoreCase)) 
            {
                for (int n = 0; n < numberOfWritingThreads; n++) 
                {
                    writingThreads[n] = new Thread(()=>QueryUpdate());
                    writingThreads[n].Start();
                }

                for (int n = 0; n < numberOfReadingThreads; n++) {
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
            return;
        }

        private void ResetDatabase()
        {
            try
            {
                Task taskFlush = cbCluster.Buckets.FlushBucketAsync(cbBucket.Name);
                Task.WaitAll(taskFlush);
                Console.WriteLine("Bucket {0} was flushed.", cbBucket.Name);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception was captured in resetting step.");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
        }

        public void KVPopulate(int TaskNumber, int StartNumber, int EndNumber) 
        {
            for(int n = StartNumber; n <= EndNumber; n++)
            {
                var tsString = DateTime.Now.ToString("s");
                string orderID = "Order_" + n.ToString();
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
                    var result = cbCollection.InsertAsync(orderID, document, options => {options.Timeout(TimeSpan.FromSeconds(60));}).Result;
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception was captured in populating task {0}.", TaskNumber);
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                    continue;
                }
            }
        }

        public void KVUpdate() {         
            Stopwatch sw = new Stopwatch();

            for(int n = 0; n < numberOfOperations; n++)
            {
                string tsString = DateTime.Now.ToString("s");
                string orderID = "Order_" + Payload.GetOrderID(1, datasetSize + 1).ToString();
                string randomOutlet = Payload.GetOutlet();
                string randomModel = Payload.GetModel();
                string randomColor = Payload.GetColor();
                double randomUnitPrice = Payload.GetUnitPrice();
                int randomQuantity = Payload.GetQuantity();

                sw.Start();
                try
                {
                    var result = cbCollection.MutateInAsync(orderID, specs => {
                                                                specs.Replace("outlet", randomOutlet);
                                                                specs.Replace("model", randomModel);
                                                                specs.Replace("color", randomColor);
                                                                specs.Replace("unitPrice", randomUnitPrice);
                                                                specs.Replace("quantity", randomQuantity);
                                                                specs.Replace("lastUpdate", tsString);
                                                            }, 
                                                            options => {options.Timeout(TimeSpan.FromSeconds(5));}).Result;
                    sw.Stop();
                    var latency = sw.ElapsedMilliseconds;
                    sw.Reset();

                    string gID = Guid.NewGuid().ToString();
                    var statsResult = cbCollection.UpsertAsync(gID, new { docType = "Stats", opType = "KVUpdate",  response = latency, lastUpdate = tsString}).Result;
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception was captured in writing tasks. OrderID {0}", orderID);
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                    continue;
                }
            }
        }

        public void KVGet() {
            Stopwatch sw = new Stopwatch();
            
            for(int n = 0; n < numberOfOperations; n++)
            {               
                string tsString = DateTime.Now.ToString("s");
                string orderID = "Order_" + Payload.GetOrderID(1, datasetSize + 1).ToString();
                sw.Start();
                try 
                {    
                    var result = cbCollection.GetAsync(orderID).Result;
                    sw.Stop();
                    var latency = sw.ElapsedMilliseconds;
                    sw.Reset();

                    string gID = Guid.NewGuid().ToString();
                    var statsResult = cbCollection.UpsertAsync(gID, new { docType = "Stats", opType = "KVGet",  response = latency, lastUpdate = tsString}).Result;
                } 
                catch (Exception e) {
                    Console.WriteLine("Exception was captured in reading tasks.");
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                    continue;
                }
            }
        }

        public void QueryUpdate() {
            Stopwatch sw = new Stopwatch();
            
            for(int n = 0; n < numberOfOperations; n++)
            {
                string orderID = "Order_" + Payload.GetOrderID(1, datasetSize + 1).ToString();
                string randomOutlet = Payload.GetOutlet();
                string randomModel = Payload.GetModel();
                string randomColor = Payload.GetColor();
                double randomUnitPrice = Payload.GetUnitPrice();
                int randomQuantity = Payload.GetQuantity();
                string tsString = DateTime.Now.ToString("s");

                // var docValue = new { docType = "Order", name = Payload.GetName(), 
                //                 gender = Payload.GetGender(), age = Payload.GetAge(), outlet = Payload.GetOutlet(), model = Payload.GetModel(),
                //                 colors = Payload.GetColor(), unitPrice = Payload.GetUnitPrice(), quantity = Payload.GetQuantity(),
                //                 txTime = tsString, lastUpdate = tsString };

                sw.Start();
                try
                {
                    var result = cbCluster.QueryAsync<dynamic>(updateSQLText,
                                                            options => options.Parameter("outlet", randomOutlet)
                                                                            .Parameter("model", randomModel)
                                                                            .Parameter("color", randomColor)
                                                                            .Parameter("unitPrice", randomUnitPrice)
                                                                            .Parameter("quantity", randomQuantity)
                                                                            .Parameter("lastUpdate", tsString)
                                                                            .Parameter("orderID", orderID)).Result;
                    sw.Stop();
                    var latency = sw.ElapsedMilliseconds;
                    sw.Reset();

                    string gID = Guid.NewGuid().ToString();
                    var statsResult = cbCollection.UpsertAsync(gID, new { docType = "Stats", opType = "QueryUpdate",  response = latency, lastUpdate = tsString}).Result;
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception was captured in writing tasks.");
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                    continue;
                }
            }
        }

        public void QueryGet() {
            Stopwatch sw = new Stopwatch();
            
            for(int n = 0; n < numberOfOperations; n++)
            {
                string orderID = "Order_" + Payload.GetOrderID(1, datasetSize + 1).ToString();
                var tsString = DateTime.Now.ToString("s");
                sw.Start();

                try {
                    var result = cbCluster.QueryAsync<dynamic>(querySQLText, options => options.Parameter("orderID", orderID)).Result;
                    sw.Stop();
                    var latency = sw.ElapsedMilliseconds;
                    sw.Reset();

                    string gID = Guid.NewGuid().ToString();
                    var statsResult = cbCollection.UpsertAsync(gID, new { docType = "Stats", opType = "QueryGet",  response = latency, lastUpdate = tsString}).Result;
                } 
                catch (Exception e) 
                {
                    Console.WriteLine("Exception was captured in reading tasks.");
                    Console.WriteLine(e.Message);
                    Console.WriteLine(e.StackTrace);
                    continue;
                }
            }
        }
    }
}
