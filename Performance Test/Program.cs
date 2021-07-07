using System;
using System.Collections.Generic;

namespace Performance_Test
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args[0].Equals("help", StringComparison.OrdinalIgnoreCase) || (args.Length < 10)) 
            {
                Console.WriteLine("Usage: ");
                Console.WriteLine("perftest.exe [Target] [Number of Populating Threads] [Number of Writing Threads] [Number Of Reading Threads] " +
                                "[Dataset Size] [Number Of Operations] [Cluster Address] [Database Name] [User Name] [Password]");
                Console.WriteLine("Options: ");
                Console.WriteLine("Target                       [mssql | cbkv | cbquery]");
                Console.WriteLine("Number of Populating Threads    0 - 200");
                Console.WriteLine("Number of Writing Threads    0 - 200");
                Console.WriteLine("Number of Reading Threads    0 - 200");
                Console.WriteLine("Dataset Size                 Number of records to be used in the test.");
                Console.WriteLine("Number of Operations         Number of operations to be executed in each thread.");
                Console.WriteLine("Cluster Address              IP address of the target cluster.");
                Console.WriteLine("Database Name                Database name.");
                Console.WriteLine("User Name                    Username.");
                Console.WriteLine("Password                     Password.");

                return;
            }

            Dictionary<string, string> testConfig = new Dictionary<string, string>();
            testConfig.Add("Target", args[0].ToLower());
            testConfig.Add("NumberOfPopulatingThreads", args[1]);
            testConfig.Add("NumberOfWritingThreads", args[2]);
            testConfig.Add("NumberOfReadingThreads", args[3]);
            testConfig.Add("DatasetSize", args[4]);
            testConfig.Add("NumberOfOperations", args[5]);
            testConfig.Add("ClusterAddress", args[6]);
            testConfig.Add("DatabaseName", args[7]);
            testConfig.Add("UserName", args[8]);
            testConfig.Add("Password", args[9]);

            if (testConfig["Target"].Equals("mssql")) 
            {
                MSSQLSrv testSrv = new MSSQLSrv(testConfig);
                testSrv.Start();
            }
            else if (testConfig["Target"].Equals("cbkv") | testConfig["Target"].Equals("cbquery"))
            {
                CBSrv cbSrv = new CBSrv(testConfig);
                cbSrv.Start();
            }
        }
    }
}
