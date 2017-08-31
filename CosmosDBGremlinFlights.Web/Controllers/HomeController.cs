using System;
using System.Collections.Generic;
using System.Configuration;
using System.Device.Location;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Mvc;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CosmosDBGremlinFlights.Web.Controllers
{
    public class HomeController : Controller
    {
        public async Task<ActionResult> Index(string from, string to)
        {
            ViewBag.JourneysJson = "[]";
            ViewBag.BingMapsKey = ConfigurationManager.AppSettings["BingMapsKey"];

            if (string.IsNullOrEmpty(from) || string.IsNullOrEmpty(to)) return View();

            using (DocumentClient client = new DocumentClient(
               new Uri(ConfigurationManager.AppSettings["CosmosDBUri"]),
               ConfigurationManager.AppSettings["CosmosDBKey"],
               new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp }))
            {
                var graph = await client.ReadDocumentCollectionAsync(ConfigurationManager.AppSettings["CosmosDBCollectionLink"]);

                var fromAirport = await GetAirportAsync(from, client, graph);
                var toAirport = await GetAirportAsync(to, client, graph);

                var routes = await GetRoutes(fromAirport, toAirport, client, graph);
                ViewBag.JourneysJson = JsonConvert.SerializeObject(routes);
            }
            return View();
        }

        private async Task<Airport> GetAirportAsync(string code, DocumentClient client, DocumentCollection graph)
        {
            var query = client.CreateGremlinQuery<Document>(graph, $"g.V('{Escape(code.ToUpperInvariant())}')");
            var results = await query.ExecuteNextAsync();
            var airportVertex = results.SingleOrDefault();
            
            if (airportVertex == null)
            {
                return null;
            }
            else
            {
                return new Airport(airportVertex);
            }
        }

        private async Task<IEnumerable<dynamic>> GetRoutes(Airport fromAirport, Airport toAirport, DocumentClient client, DocumentCollection graph)
        {
            const double maxDistanceFactor = 1.5;

            var from = Escape(fromAirport.Code);
            var to = Escape(toAirport.Code);
            var distance = GetDistance(fromAirport, toAirport);

            var query = client.CreateGremlinQuery<Document>(graph, $"g.V('{from}').union(outE().inV().hasId('{to}'), outE().inV().outE().inV().hasId('{to}')).path()");
            IEnumerable<Journey> allJourneys = new List<Journey>(); 
            while (query.HasMoreResults)
            {
                var results = await query.ExecuteNextAsync();
                var journeys = results.Select(r => GetJourney((JArray)r.objects));
                allJourneys = allJourneys.Concat(journeys);
            }
            return allJourneys.Where(j => j.TotalDistance < distance * maxDistanceFactor);
        }

        private Journey GetJourney(JArray path)
        {
            return new Journey
            {
                TotalDistance = path.Where(i => i["label"].Value<string>() == "flight").Cast<dynamic>().Sum(f => (double)f.properties.distance.Value),
                Airports = path.Where(i => i["label"].Value<string>() == "airport").Select(i => new Airport(i)).ToArray()
            };
        }

        private double GetDistance(Airport fromAirport, Airport toAirport)
        {
            var from = new GeoCoordinate(fromAirport.Latitude, fromAirport.Longitude);
            var to = new GeoCoordinate(toAirport.Latitude, toAirport.Longitude);
            return from.GetDistanceTo(to);
        }

        private string Escape(string input)
        {
            return input?.Replace("'", @"\'")?.Replace("\"", "\\\"");
        }
    }
}