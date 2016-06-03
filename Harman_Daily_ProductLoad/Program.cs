using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace Harman_Daily_ProductLoad
{
    class Program
    {
        static void Main(string[] args)
        {
            //move to app.config    
            string filePath = ConfigurationManager.AppSettings["FilePath"]; 
            string archivePath = ConfigurationManager.AppSettings["ArchivePath"]; 
            bool archive = System.Convert.ToBoolean(ConfigurationManager.AppSettings["ArchiveFile"]);
            const Int32 BufferSize = 1024;
            var files = Directory.GetFiles(filePath);
            int filecount = 0;

            log4net.Config.BasicConfigurator.Configure();
            ILog log = log4net.LogManager.GetLogger(typeof(Program));

            log.Info("Starting...");
            try
            {
                //move connection to app.config
                using (SqlConnection con = new SqlConnection(ConfigurationManager.ConnectionStrings["WebImport_Staging"].ConnectionString))
                {
                    //connect to DB
                    con.Open();

                    log.Info("Connected to database");

                    //clear table
                    using (var cmd = con.CreateCommand())
                    {
                        try
                        {
                            cmd.CommandText = "DELETE FROM HRMN_daily_data";
                            cmd.ExecuteNonQuery();
                            log.Info("Data deleted from HRMN_daily_data staging table");
                        }
                        catch (Exception ex)
                        {
                            log.Error(ex.Message);
                        }

                    }

                    foreach (var file in files)
                    {
                        log.Info("Processing " + file.ToString());
                        log.Info("Executing sp_Add_Harman_Record sproc");

                        //increment filecount
                        filecount++;  

                        using (var fileStream = File.OpenRead(file))
                        using (var streamReader = new StreamReader(fileStream, Encoding.UTF8, true, BufferSize))
                        {
                            String line;
                            while ((line = streamReader.ReadLine()) != null)
                            {
                                // Ignore Header (H3) and Footer (T3) lines
                                if (line.Substring(0, 1) != "H" && line.Substring(0, 1) != "T")
                                    // Process line
                                    using (SqlCommand cmd = new SqlCommand("sp_Add_Harman_Record", con))
                                    {
                                        cmd.CommandType = CommandType.StoredProcedure;

                                        cmd.Parameters.Add("@action", SqlDbType.VarChar).Value = line.Substring(0, 1).Trim();
                                        cmd.Parameters.Add("@partno", SqlDbType.VarChar).Value = line.Substring(1, 96).Trim();
                                        cmd.Parameters.Add("@hts", SqlDbType.VarChar).Value = line.Substring(97, 36).Trim();
                                        cmd.Parameters.Add("@partdesc", SqlDbType.VarChar).Value = line.Substring(133, 30).Trim();
                                        cmd.Parameters.Add("@vendorcode", SqlDbType.VarChar).Value = line.Substring(165, 13).Trim();
                                        cmd.Parameters.Add("@spi", SqlDbType.VarChar).Value = line.Substring(178, 4).Trim();
                                        cmd.Parameters.Add("@spibegindate", SqlDbType.VarChar).Value = line.Substring(182, 8).Trim();
                                        cmd.Parameters.Add("@spienddate", SqlDbType.VarChar).Value = line.Substring(190, 8).Trim();
                                        cmd.Parameters.Add("@netcost", SqlDbType.VarChar).Value = line.Substring(198, 1).Trim();
                                        cmd.Parameters.Add("@naftaeligibility", SqlDbType.VarChar).Value = line.Substring(817, 1).Trim();

                                        //con.Open();
                                        try
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                        catch(Exception ex)
                                        {
                                            log.Error(ex.Message);
                                        }
                                        
                                    }
                                else
                                    Console.WriteLine("Skipping Header or Footer");
                            }
                        }
                        //call Load Stored Procedure to load data to tables
                        using (var cmd = con.CreateCommand())
                        {
                            try
                            {
                                cmd.CommandText = "BDP_LoadHarmanDailyData";
                                cmd.ExecuteNonQuery();
                                log.Info("Executing BDP_LoadHarmanDailyData stored procedure");
                            }
                            catch (Exception ex)
                            {
                                log.Error(ex.Message);
                            }

                        }

                        if (archive == true)
                        {
                            //archive file on success
                            string fileName = file.Substring(file.LastIndexOf(@"\"), file.Length - file.LastIndexOf(@"\"));

                            //rename existing file if exists in archive path
                            if (File.Exists(archivePath + fileName))
                            {
                                File.Delete(archivePath + fileName);
                            }
                            //archive file
                            File.Move(filePath + fileName, archivePath + fileName);
                            log.Info("Archiving " + fileName + " to " + archivePath);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                //log error message
                log.Error(ex.Message);
            }
            finally
            {
                files = null;
                //Console.ReadKey();
            }

            if (filecount == 0)
                log.Info("Nothing to process!");
            else
                log.Info("Processed " + filecount.ToString() + " files.");

            //quit program
            log.Info("Ending...");
             
         }
    }
}
