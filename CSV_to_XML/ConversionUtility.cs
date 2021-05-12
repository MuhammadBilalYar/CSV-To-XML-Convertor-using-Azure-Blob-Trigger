using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic.FileIO;

namespace CSV_to_XML
{
    public class ConversionUtility
    {
        private const string ContainerPath = "bisolutioncontainer210227";
        /// <summary>
        /// Utility method to convert CSV data to XML
        /// </summary>
        /// <param name="csvData">List of records in CSV</param>
        /// <param name="header">Array of column headers </param>
        /// <param name="log">Logger</param>
        /// <returns>Stream to XML data</returns>
        public static Stream ConvertToXML(List<string> csvData, string[] header, ILogger log)
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

        public static Stream DownloadCsv(string blobId, ILogger log)
        {
            blobId = $"csv/{blobId}";
            BlobClient blobClient = GetContainer(log).GetBlobClient(blobId);
            log.LogInformation("Loading CSV blob to memory.");
            BlobDownloadInfo download = blobClient.Download();

            MemoryStream downloadFileStream = new MemoryStream();
            download.Content.CopyTo(downloadFileStream);
            log.LogInformation("CSV file content loaded.");
            downloadFileStream.Position = 0;
            return downloadFileStream;
        }

        /// <summary>
        /// Utility to store data to blob
        /// </summary>
        /// <param name="stream">XML data stream</param>
        /// <param name="xmlFileName"> XML file name (Blob Name)</param>
        /// <param name="log">Logger</param>
        public static void SaveData(Stream stream, string xmlFileName, ILogger log)
        {
            string blobId = $"xml/{xmlFileName}";
            BlobClient blobClient = GetContainer(log).GetBlobClient(blobId);
            if (blobClient.Exists())
            {
                log.LogInformation("Deleting existing file and will generate new one.");
                blobClient.Delete();
            }

            stream.Position = 0;
            BlobContentInfo info = blobClient.Upload(stream);

            log.LogInformation("XML created.");
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
