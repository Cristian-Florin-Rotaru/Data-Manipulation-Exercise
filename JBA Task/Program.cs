using System;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Data.SqlClient;
using System.Data;

// ADDED TO REFERENCE "System.Windows.Forms" in order to use the open file window 
// 
namespace JBA_Task
{
    class JBATask
    {
        static string textFile = "";
        static string dbConn = @"Server =.\SQLEXPRESS; Database = jbaDB; Trusted_Connection = True;";
        static string serverConn = @"Server =.\SQLEXPRESS; Trusted_Connection = True;";
        static string table = "CREATE TABLE PRECIPITATION (ID int IDENTITY(1,1) PRIMARY KEY, Xref int, Yref int, Date DATE, Value int)";

        public static int startYear = 0;
        public static int endYear = 0;
        public static int xRef = 0;
        public static int yRef = 0;
        public static int yearIndex = 0;
        public static int monthIndex = 0;

        [STAThread]
        static void Main(string[] args)
        {

            JBATask jba = new JBATask();
            jba.createDatabase();

            SqlConnection conn = new SqlConnection(dbConn);
            try
            {
                conn.Open();
                jba.createTable(conn);
                jba.setFilePath();
                jba.processFile(conn);


            }
            catch (System.Exception ex)
            {
                MessageBox.Show(ex.ToString(), "MyProgram", MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }

            

            Console.ReadKey();
        }


        public void processFile(SqlConnection conn)
        {
            if (File.Exists(textFile))
            {
                try
                {
                    StreamReader file = new StreamReader(textFile);

                    string ln;
                    bool searchYears = true;
                    int currentLine = 0;

                    Console.WriteLine("Started exporting data to DATABASE, Please wait");
                    while ((ln = file.ReadLine()) != null)
                    {
                        currentLine++;
                        if (searchYears && currentLine > 10)
                        {
                            throw new System.ArgumentException("Period of time not found");
                        }
                   
                        if (!searchYears && !ln.Contains("Grid-ref=") && yearIndex <= endYear)
                        {
                            sendToDB(ln, conn);
                            yearIndex++;

                        }

                        if (searchYears && ln.Contains("Years="))
                        {
                            searchYears = false;
                            setYears(ln);
                            if (startYear > DateTime.Now.Year || startYear < 1900 || endYear > DateTime.Now.Year || endYear < 1900 || startYear > endYear )
                                throw new System.ArgumentException("Provided wrong information about the period of time");

                        }

                        if (!searchYears && ln.Contains("Grid-ref="))
                        {
                            setGridRef(ln);
                        }

                    }
                    file.Close();
                    Console.WriteLine("Finished exporting data to DATABASE");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }

        /*
         * Opens a file dialog to pick up the file wanted to process
         **/

        public void setFilePath()
        {
            OpenFileDialog ofd = new OpenFileDialog();
            ofd.ShowDialog();
            textFile = ofd.FileName;
        }
        /*
         * Sets the years the data from the file was recorded 
         **/
        public void setYears(string ln)
        {
            string years = ln.Split(new string[] { "Years=" }, StringSplitOptions.None)[1].Split(']')[0];
            int.TryParse(years.Split('-')[0], out startYear);
            int.TryParse(years.Split('-')[1], out endYear);
        }

        /*
         * Sets the GridRef given before each set of data
         **/
        public void setGridRef(string ln)
        {
            ln = ln.Split(new string[] { "Grid-ref=" }, StringSplitOptions.None)[1].Replace(" ", "");
            int.TryParse(ln.Split(',')[0], out xRef);
            int.TryParse(ln.Split(',')[1], out yRef);
            yearIndex = startYear;
        }
        /* Has a line of data(12 months data) string given and 
         * returns an int[12] array with the data 
         **/
        public int[] dataLineToArray(string ln)
        {
            int[] monthValues = new int[12];
            monthIndex = 0;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < ln.Length; i++)
            {
                if (!ln[i].Equals(' '))
                {
                    sb.Append(ln[i]);
                }

                if (i % 5 == 4)
                {
                    int.TryParse(sb.ToString(), out monthValues[monthIndex]);
                    monthIndex++;
                    sb = new StringBuilder();
                }

            }
            return monthValues;
        }

        /* Logs into the database a year of data with the x and y ref
         * 
         **/
        public void sendToDB(string ln, SqlConnection conn)
        {
            string sql = "INSERT INTO PRECIPITATION values (@xRef, @yRef, @Date, @Value)";
            SqlCommand cmd = new SqlCommand(sql, conn);
            int[] values = dataLineToArray(ln);
            string date = "";
            for (int i = 1; i <= values.Length; i++)
            {
                date = yearIndex + "-" + i + "-1";
                cmd = new SqlCommand(sql, conn);
                cmd.Parameters.Add("@xRef", SqlDbType.Int);
                cmd.Parameters.Add("@yRef", SqlDbType.Int);
                cmd.Parameters.Add("@Date", SqlDbType.Date);
                cmd.Parameters.Add("@Value", SqlDbType.Int);
                cmd.Parameters["@xRef"].Value = xRef;
                cmd.Parameters["@yRef"].Value = yRef;
                cmd.Parameters["@Date"].Value = date;
                cmd.Parameters["@Value"].Value = values[i-1];
                cmd.ExecuteNonQuery();
            }
            
          
        }
        /*
         * Creates the database on a SQL server 
         **/
        public void createDatabase()
        {
            String str = "CREATE DATABASE jbaDB";
            SqlConnection conn = new SqlConnection(serverConn);
            SqlCommand myCommand = new SqlCommand(str, conn);
            try
            {
                conn.Open();
                myCommand.ExecuteNonQuery();
                Console.WriteLine("DataBase is Created Successfully");
                myCommand = new SqlCommand(table, conn);

            }
            catch (System.Exception ex)
            {
                MessageBox.Show(ex.ToString(), "MyProgram", MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
        }

        public void createTable(SqlConnection conn)
        {
            try
            {
                {
                    SqlCommand cmd = new SqlCommand(table, conn);
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("Table is Created Successfully");
                }
            }
            catch (Exception ex)
            {
                //display error message
                MessageBox.Show(ex.ToString(), "MyProgram", MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
        }
    }
}
