namespace CosmosDBGremlinFlights.Web
{
    public class Airport
    {
        public string Code { get; internal set; }
        public double Latitude { get; internal set; }
        public double Longitude { get; internal set; }

        public Airport(dynamic airportVertex)
        {
            Code = airportVertex.id;
            Latitude = airportVertex.properties.latitude[0].value;
            Longitude = airportVertex.properties.longitude[0].value;
        }
    }
}