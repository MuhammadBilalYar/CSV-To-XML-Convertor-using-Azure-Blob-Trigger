using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Linq;

namespace CSV_to_XML
{
    public class JsonBodyModel
    {
        public string FileName { get; set; }
    }


    public static class ConvertXmlToCsvHTTP
    {
        [FunctionName("ConvertXmlToCsvHTTP")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            log.LogInformation($"Request Data: {requestBody}");
            JsonBodyModel data = JsonConvert.DeserializeObject<JsonBodyModel>(requestBody);
            Stream inputFile = ConversionUtility.DownloadCsv(data.FileName, log);

            StreamReader reader = new StreamReader(inputFile);
            string content = reader.ReadToEnd();
            var csvData = content.Split("\n").ToList();
            log.LogInformation($"Total records in blob {csvData.Count}");
            if (csvData.Count > 1) // Must have at least one record
            {
                string[] header = csvData.FirstOrDefault().Replace("\r", "").Split(',');
                csvData.RemoveAt(0);


                var stream = ConversionUtility.ConvertToXML(csvData, header, log);
                log.LogInformation("Data converted to XML");

                log.LogInformation("Saving XML to blob storage");
                string xmlFileName = $"{Path.GetFileNameWithoutExtension(data.FileName)}.xml";
                ConversionUtility.SaveData(stream, xmlFileName, log);

                log.LogInformation("Completed");

            }
            else
            {
                log.LogInformation("CSV Must have at least one record.");
            }

            return new OkObjectResult("{  \"Message\": \"Conversion Completed\"}");
        }
    }
}
