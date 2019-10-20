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
        static string createTableSQL = "CREATE TABLE PRECIPITATION (ID int IDENTITY(1,1) PRIMARY KEY, Xref int, Yref int, Date DATE, Value int)";
        static string tableName = "PRECIPITATION";
        static int startYear = 0;
        static int endYear = 0;
        static int xRef = 0;
        static int yRef = 0;
        static int yearIndex = 0;
        static int monthIndex = 0;
        static DataTable dataTable;
        static long dtSize = 0;
        

        [STAThread]
        static void Main(string[] args)
        {

            JBATask jba = new JBATask();
            jba.CreateDatabase();

            SqlConnection conn = new SqlConnection(dbConn);
            try
            {
                conn.Open();
                jba.CreateTable(conn);
                jba.SetFilePath();
                jba.ProcessFile(conn);

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


        public void ProcessFile(SqlConnection conn)
        {
            if (File.Exists(textFile))
            {
                try
                {
                    StreamReader file = new StreamReader(textFile);
                    SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(conn);
                    sqlBulkCopy.DestinationTableName = tableName;
                    InitiateDataTable();
                    string ln;
                    bool searchYears = true;
                    long currentLine = 0;
                    Console.WriteLine("Started exporting data to DATABASE AT:" + DateTime.Now);
                    while ((ln = file.ReadLine()) != null)
                    {
                        currentLine++;
                        if (searchYears && currentLine > 10)
                        {
                            throw new System.ArgumentException("Period of time not found");
                        }

                        if (dtSize > 50000)
                        {
                            sqlBulkCopy.WriteToServer(dataTable);
                            InitiateDataTable();
                            dtSize = 0;
                        }

                            if (!searchYears && !ln.Contains("Grid-ref=") && yearIndex <= endYear)
                        {
                            FillDataTable(ln);
                            yearIndex++;
                        }

                        if (searchYears && ln.Contains("Years="))
                        {
                            searchYears = false;
                            SetYears(ln);
                            if (startYear > DateTime.Now.Year || startYear < 1900 || endYear > DateTime.Now.Year || endYear < 1900 || startYear > endYear )
                                throw new System.ArgumentException("Provided wrong information about the period of time");
                        }

                        if (!searchYears && ln.Contains("Grid-ref="))
                        {
                            SetGridRef(ln);
                        }

                    }
                    file.Close();
                    sqlBulkCopy.WriteToServer(dataTable);
                    Console.WriteLine("Finished exporting data to DATABASE");
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }
        public void InitiateDataTable()
        {
            dataTable = new DataTable();
            dataTable.Columns.Add("ID");
            dataTable.Columns.Add("Xref");
            dataTable.Columns.Add("yRef");
            dataTable.Columns.Add("Date");
            dataTable.Columns.Add("Value");

        }
        public void FillDataTable(string ln)
        {
            int[] values = DataLineToArray(ln);
            string date;
            for (int i = 1; i <= values.Length; i++)
            {
                date = yearIndex + "-" + i + "-1";
                dataTable.Rows.Add(new Object[] { "", xRef, yRef, date, values[i - 1] });
                dtSize++;
            }
        }

        /*
         * Opens a file dialog to pick up the file wanted to process
         **/

        public void SetFilePath()
        {
            OpenFileDialog ofd = new OpenFileDialog();
            ofd.ShowDialog();
            textFile = ofd.FileName;
        }
        /*
         * Sets the years the data from the file was recorded 
         **/
        public void SetYears(string ln)
        {
            string years = ln.Split(new string[] { "Years=" }, StringSplitOptions.None)[1].Split(']')[0];
            int.TryParse(years.Split('-')[0], out startYear);
            int.TryParse(years.Split('-')[1], out endYear);
        }

        /*
         * Sets the GridRef given before each set of data
         **/
        public void SetGridRef(string ln)
        {
            ln = ln.Split(new string[] { "Grid-ref=" }, StringSplitOptions.None)[1].Replace(" ", "");
            int.TryParse(ln.Split(',')[0], out xRef);
            int.TryParse(ln.Split(',')[1], out yRef);
            yearIndex = startYear;
        }

        /* Has a line of data(12 months data) string given and 
         * returns an int[12] array with the data 
         **/
        public int[] DataLineToArray(string ln)
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

        /*
         * Creates the database on a SQL server 
         **/
        public void CreateDatabase()
        {
            String str = "CREATE DATABASE jbaDB";
            SqlConnection conn = new SqlConnection(serverConn);
            SqlCommand myCommand = new SqlCommand(str, conn);
            try
            {
                conn.Open();
                myCommand.ExecuteNonQuery();
                Console.WriteLine("DataBase is Created Successfully");
                myCommand = new SqlCommand(createTableSQL, conn);

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

        public void CreateTable(SqlConnection conn)
        {
            try
            {
                {
                    SqlCommand cmd = new SqlCommand(createTableSQL, conn);
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

        /* **Used bulk insert instead as it took too much time to insert all the data with one insert at a time**
        * Logs into the database a year of data with the x and y ref
        * 
       public void SendToDB(string ln, SqlConnection conn)
       {
           string sql = "INSERT INTO PRECIPITATION values (@xRef, @yRef, @Date, @Value)";
           string sqlv2 = "INSERT INTO PRECIPITATION values ({0}, {1}, '{2}', {3});";
           StringBuilder sb = new StringBuilder();
           // SqlCommand cmd = new SqlCommand(sql, conn);
           SqlCommand cmd;
           string date = "";

           int[] values = DataLineToArray(ln);
           for (int i = 1; i <= values.Length; i++)
           {
               date = yearIndex + "-" + i + "-1";
              // sb.AppendFormat(sqlv2, xRef, yRef, date, values[i - 1]);
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
          // cmd = new SqlCommand(sb.ToString(), conn);
          // cmd.ExecuteNonQuery();

       }
       */
    }
}
