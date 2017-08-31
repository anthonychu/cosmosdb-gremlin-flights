using System.Collections.Generic;

namespace CosmosDBGremlinFlights.Web
{
    public class Journey
    {
        public double TotalDistance { get; set; }
        public IEnumerable<Airport> Airports { get; set; }
    }
}