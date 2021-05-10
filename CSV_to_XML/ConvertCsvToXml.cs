using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic.FileIO;

namespace CSV_to_XML
{
    /// <summary>
    /// An Azure function to convert CSV file to XML
    /// </summary>
    public static class ConvertCsvToXml
    {
        private const string ContainerPath = "bisolutioncontainer210227";

        /// <summary>
        /// Azure Function
        /// </summary>
        /// <param name="myBlob"> Newly added blob stream</param>
        /// <param name="name">new added blob name</param>
        /// <param name="log">Function logger</param>
        /// 
        [FunctionName("ConvertCsvToXml")]
        public static void Run([BlobTrigger(ContainerPath + "/csv/{name}", Connection = "StorageConnectionAppSetting")] Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            StreamReader reader = new StreamReader(myBlob);
            string content = reader.ReadToEnd();
            var csvData = content.Split("\n").ToList();
            log.LogInformation($"Total records in blob {csvData.Count}");
            if (csvData.Count > 1 && name.ToLower().EndsWith(".csv")) // Must have at least one record
            {
                string[] header = csvData.FirstOrDefault().Replace("\r", "").Split(',');
                csvData.RemoveAt(0);

                
                var stream = ConvertToXML(csvData, header, log);
                log.LogInformation("Data converted to XML");

                log.LogInformation("Saving XML to blob storage");
                string xmlFileName = $"{Path.GetFileNameWithoutExtension(name)}.xml";
                SaveData(stream, xmlFileName, log);

                log.LogInformation("Completed");

            }
            else
            {
                log.LogInformation("CSV Must have at least one record.");
            }
        }

        /// <summary>
        /// Utility method to convert CSV data to XML
        /// </summary>
        /// <param name="csvData">List of records in CSV</param>
        /// <param name="header">Array of column headers </param>
        /// <param name="log">Logger</param>
        /// <returns>Stream to XML data</returns>
        private static Stream ConvertToXML(List<string> csvData, string[] header, ILogger log)
        {
            try
            {
                var stream = new MemoryStream();
                XmlTextWriter writer = new XmlTextWriter(stream, Encoding.UTF8);


                writer.WriteStartDocument();
                writer.WriteStartElement("Root");

                foreach (var item in csvData)
                {
                    if (string.IsNullOrEmpty(item))
                        continue;

                    TextFieldParser parser = new TextFieldParser(new StringReader(item.Replace("\r", "")));
                    parser.HasFieldsEnclosedInQuotes = true;
                    parser.SetDelimiters(",");
                    var data = parser.ReadFields();

                    if (header.Length == data.Length)
                    {
                        writer.WriteStartElement("record");
                        for (int i = 0; i < data.Length; i++)
                        {
                            string attibute = header[i].ToString();
                            string value = data[i].ToString();
                            writer.WriteElementString(attibute, value);
                        }
                        writer.WriteEndElement();
                    }
                    else
                    {
                        log.LogInformation("Mis match");
                    }
                }
                writer.WriteEndElement();
                writer.WriteEndDocument();
                return stream;
            }
            catch (Exception ex)
            {
                log.LogError($"Unable to convert to XML: {ex.Message}", ex);
                throw ex;
            }
        }

        #region Blob Storage

        /// <summary>
        /// Utility to store data to blob
        /// </summary>
        /// <param name="stream">XML data stream</param>
        /// <param name="xmlFileName"> XML file name (Blob Name)</param>
        /// <param name="log">Logger</param>
        private static void SaveData(Stream stream, string xmlFileName, ILogger log)
        {
            string blobId = $"xml/{xmlFileName}";
            BlobClient blobClient = GetContainer(log).GetBlobClient(blobId);
            if (!blobClient.Exists())
            {
                stream.Position = 0;
                BlobContentInfo info = blobClient.Upload(stream);

                log.LogInformation("XML created.");
            }
            else
            {
                log.LogInformation("A file with same name already exist.");
            }
        }

        /// <summary>
        /// Utility to connect with container
        /// </summary>
        /// <param name="log">Logger</param>
        /// <returns>Container Client</returns>
        private static BlobContainerClient GetContainer(ILogger log)
        {
            string connectionString = Environment.GetEnvironmentVariable("StorageConnectionAppSetting");
            BlobContainerClient containerClient = new BlobContainerClient(connectionString, ContainerPath);
            containerClient.CreateIfNotExists(PublicAccessType.None);
            log.LogInformation($"Container connected to {ContainerPath}");
            return containerClient;
        }


        #endregion
    }
}
