using System;

namespace Performance_Test
{
    static class Payload
    {
        private static Random rand = new Random();
        private static object randLock = new Object();
        private static string[] names = {"Liam", "Noah", "Oliver", "William", "Elijah", 
                                        "James", "Benjamin", "Lucas", "Mason", "Ethan", 
                                        "Alexander", "Henry", "Jacob", "Michael", "Daniel", 
                                        "Logan", "Jackson", "Sebastian", "Jack", "Aiden", 
                                        "Owen", "Samuel", "Matthew", "Joseph", "Levi", "Mateo", 
                                        "David", "John", "Wyatt", "Carter", "Julian", "Luke", 
                                        "Grayson", "Isaac", "Jayden", "Theodore", "Gabriel", 
                                        "Anthony", "Dylan", "Leo", "Lincoln", "Jaxon", "Asher", 
                                        "Christopher", "Josiah", "Andrew", "Thomas", "Joshua", 
                                        "Ezra", "Hudson", "Charles", "Caleb", "Isaiah", "Ryan", 
                                        "Nathan", "Adrian", "Christian", "Maverick", "Colton", 
                                        "Elias", "Aaron", "Eli", "Landon", "Jonathan", "Nolan", 
                                        "Hunter", "Cameron", "Connor", "Santiago", "Jeremiah", 
                                        "Ezekiel", "Angel", "Roman", "Easton", "Miles", "Robert", 
                                        "Jameson", "Nicholas", "Greyson", "Cooper", "Ian", 
                                        "Carson", "Axel", "Jaxson", "Dominic", "Leonardo", "Luca", 
                                        "Austin", "Jordan", "Adam", "Xavier", "José", "Jace", 
                                        "Everett", "Declan", "Evan", "Kayden", "Parker", "Wesley", 
                                        "Kai", "Brayden", "Bryson", "Weston", "Jason", "Emmett", 
                                        "Sawyer", "Silas", "Bennett", "Brooks", "Micah", "Damian", 
                                        "Harrison", "Waylon", "Ayden", "Vincent", "Ryder", 
                                        "Kingston", "Rowan", "George", "Luis", "Chase", "Cole", 
                                        "Nathaniel", "Zachary", "Ashton", "Braxton", "Gavin", 
                                        "Tyler", "Diego", "Bentley", "Amir", "Beau", "Gael", 
                                        "Carlos", "Ryker", "Jasper", "Max", "Juan", "Ivan", 
                                        "Brandon", "Jonah", "Giovanni", "Kaiden", "Myles", "Calvin", 
                                        "Lorenzo", "Maxwell", "Jayce", "Kevin", "Legend", "Tristan", 
                                        "Jesus", "Jude", "Zion", "Justin", "Maddox", "Abel", "King", 
                                        "Camden", "Elliott", "Malachi", "Milo", "Emmanuel", "Karter", 
                                        "Rhett", "Alex", "August", "River", "Xander", "Antonio", 
                                        "Brody", "Finn", "Elliot", "Dean", "Emiliano", "Eric", 
                                        "Miguel", "Arthur", "Matteo", "Graham", "Alan", "Nicolas", 
                                        "Blake", "Thiago", "Adriel", "Victor", "Joel", "Timothy", 
                                        "Hayden", "Judah", "Abraham", "Edward", "Messiah", "Zayden", 
                                        "Theo", "Tucker", "Grant", "Richard", "Alejandro", "Steven", 
                                        "Jesse", "Dawson", "Bryce", "Avery", "Oscar", "Patrick", 
                                        "Archer", "Barrett", "Leon", "Colt", "Charlie", "Peter", 
                                        "Kaleb", "Lukas", "Beckett", "Jeremy", "Preston", "Enzo", 
                                        "Luka", "Andres", "Marcus", "Felix", "Mark", "Ace", "Brantley", 
                                        "Atlas", "Remington", "Maximus", "Matias", "Walker", "Kyrie", 
                                        "Griffin", "Kenneth", "Israel", "Javier", "Kyler", "Jax", "Amari", 
                                        "Zane", "Emilio", "Knox", "Adonis", "Aidan", "Kaden", "Paul", 
                                        "Omar", "Brian", "Louis", "Caden", "Maximiliano", "Holden", 
                                        "Paxton", "Nash", "Bradley", "Bryan", "Simon", "Phoenix", "Lane", 
                                        "Josue", "Colin", "Rafael", "Kyle", "Riley", "Jorge", "Beckham", 
                                        "Cayden", "Jaden", "Emerson", "Ronan", "Karson", "Arlo", "Tobias", 
                                        "Brady", "Clayton", "Francisco", "Zander", "Erick", "Walter", "Daxton", 
                                        "Cash", "Martin", "Damien", "Dallas", "Cody", "Chance", "Jensen", 
                                        "Finley", "Jett", "Corbin", "Kash", "Reid", "Kameron", "Andre", 
                                        "Gunner", "Jake", "Hayes", "Manuel", "Prince", "Bodhi", "Cohen", 
                                        "Sean", "Khalil", "Hendrix", "Derek", "Cristian", "Cruz", "Kairo", 
                                        "Dante", "Atticus", "Killian", "Stephen", "Orion", "Malakai", "Ali", 
                                        "Eduardo", "Fernando", "Anderson", "Angelo", "Spencer", "Gideon", 
                                        "Mario", "Titus", "Travis", "Rylan", "Kayson", "Ricardo", "Tanner", 
                                        "Malcolm", "Raymond", "Odin", "Cesar", "Lennox", "Joaquin", "Kane", 
                                        "Wade", "Muhammad", "Iker", "Jaylen", "Crew", "Zayn", "Hector", 
                                        "Ellis", "Leonel", "Cairo", "Garrett", "Romeo", "Dakota", "Edwin", 
                                        "Warren", "Julius", "Major", "Donovan", "Caiden", "Tyson", "Nico", 
                                        "Sergio", "Nasir", "Rory", "Devin", "Jaiden", "Jared", "Kason", 
                                        "Malik", "Jeffrey", "Ismael", "Elian", "Marshall", "Lawson", "Desmond", 
                                        "Winston", "Nehemiah", "Ari", "Conner", "Jay", "Kade", "Andy", "Johnny", 
                                        "Jayceon", "Marco", "Seth", "Ibrahim", "Raiden", "Collin", "Edgar", "Erik", 
                                        "Troy", "Clark", "Jaxton", "Johnathan", "Gregory", "Russell", "Royce", 
                                        "Fabian", "Ezequiel", "Noel", "Pablo", "Cade", "Pedro", "Sullivan", 
                                        "Trevor", "Reed", "Quinn", "Frank", "Harvey", "Princeton", "Zayne", 
                                        "Matthias", "Conor", "Sterling", "Dax", "Grady", "Cyrus", "Gage", 
                                        "Leland", "Solomon", "Emanuel", "Niko", "Ruben", "Kasen", "Mathias", 
                                        "Kashton", "Franklin", "Remy", "Shane", "Kendrick", "Shawn", "Otto", 
                                        "Armani", "Keegan", "Finnegan", "Memphis", "Bowen", "Dominick", "Kolton", 
                                        "Jamison", "Allen", "Philip", "Tate", "Peyton", "Jase", "Oakley", "Rhys", 
                                        "Kyson", "Adan", "Esteban", "Dalton", "Gianni", "Callum", "Sage", "Alexis", 
                                        "Milan", "Moises", "Jonas", "Uriel", "Colson", "Marcos", "Zaiden", "Hank", 
                                        "Damon", "Hugo", "Ronin", "Royal", "Kamden", "Dexter", "Luciano", "Alonzo", 
                                        "Augustus", "Kamari", "Eden", "Roberto", "Baker", "Bruce", "Kian", "Albert", 
                                        "Frederick", "Mohamed", "Abram", "Omari", "Porter", "Enrique", "Alijah", 
                                        "Francis", "Leonidas", "Zachariah", "Landen", "Wilder", "Apollo", "Santino", 
                                        "Tatum", "Pierce", "Forrest", "Corey", "Derrick", "Isaias", "Kaison", "Kieran", 
                                        "Arjun", "Gunnar", "Rocco", "Emmitt"};
        private static string[] genders = {"male", "female"};
        private static string[] colors = {"black", "silver", "blue", "rose", "pink", "yellow", "red", "rainbow"};

        public static int GetOrderID(int Start, int End) 
        {
            int n;
            // DateTimeOffset dt = DateTimeOffset.UtcNow;
            // long ms = dt.ToUnixTimeMilliseconds();
            // int n = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() % (End - Start);
            lock (randLock)
            {
                n = rand.Next(Start, End + 1);
            }
            return n;
        }

        public static string GetName()
        {
            int n;
            // n = DateTime.Now.Millisecond % names.Length;
            lock (randLock)
            {
                n = rand.Next(0, names.Length);
            }
            return names[n];
        }

        public static string GetGender()
        {
            int n;
            // n = DateTime.Now.Millisecond % genders.Length;
            lock (randLock)
            {
                n = rand.Next(0, genders.Length);
            }
            return genders[n];
        }

        public static int GetAge()
        {
            int n;
            // n = 16 + DateTime.Now.Millisecond % 10;
            lock (randLock)
            {
                n = rand.Next(16, 100);
            }
            return rand.Next(n);
        }

        public static string GetModel()
        {
            int n;
            // n = 100 + DateTime.Now.Millisecond % 100;
            lock (randLock)
            {
                n = rand.Next(0, 100);
            }
            return "Model" + n.ToString();
        }

        public static string GetColor()
        {
            int n;
            // n = DateTime.Now.Millisecond % colors.Length;
            lock (randLock)
            {
                n = rand.Next(0, colors.Length);
            }
            return colors[n];
        }

        public static string GetOutlet()
        {
            int n;
            // n = 1000 + DateTime.Now.Millisecond;
            lock (randLock)
            {
                n = rand.Next(0, 1000);
            }
            return "Outlet" + n.ToString();
        }

        public static double GetUnitPrice()
        {
            int n;
            // n = DateTime.Now.Millisecond;
            lock (randLock)
            {
                n = rand.Next(0, 100000);
            }
            return  n / 100.0;
        }

        public static double GetDiscount()
        {
            int n;
            // n = DateTime.Now.Millisecond % 10;
            lock (randLock)
            {
                n = rand.Next(0, 50);
            }
            return  n / 100.0;
        }

        public static int GetQuantity()
        {
            int n;
            // n = DateTime.Now.Millisecond;
            lock (randLock)
            {
                n = rand.Next(0, 1000);
            }
            return  n;
        }
    }
}
