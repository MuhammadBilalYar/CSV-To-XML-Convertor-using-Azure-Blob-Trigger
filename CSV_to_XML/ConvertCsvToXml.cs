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
        [Disable]
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

                
                var stream = ConversionUtility.ConvertToXML(csvData, header, log);
                log.LogInformation("Data converted to XML");

                log.LogInformation("Saving XML to blob storage");
                string xmlFileName = $"{Path.GetFileNameWithoutExtension(name)}.xml";
                ConversionUtility.SaveData(stream, xmlFileName, log);

                log.LogInformation("Completed");

            }
            else
            {
                log.LogInformation("CSV Must have at least one record.");
            }
        }

    }
}
