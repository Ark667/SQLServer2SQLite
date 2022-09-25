namespace SqlServer2SqLite.Core.Models
{
    public class TriggerSchema
    {
        public string Name { get; set; }
        public TriggerEvent Event { get; set; }
        public TriggerType Type { get; set; }
        public string Body { get; set; }
        public string Table { get; set; }

        public override string ToString()
        {
            return @"CREATE TRIGGER ["
                + Name
                + "] "
                + Type
                + " "
                + Event
                + " ON ["
                + Table
                + "] "
                + "BEGIN "
                + Body
                + " END;";
        }
    }
}
