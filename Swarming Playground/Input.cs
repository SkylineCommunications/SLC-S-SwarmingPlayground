namespace Swarming_Playground
{
    using Newtonsoft.Json;
    using Skyline.DataMiner.Automation;
    using System.Linq;
    using System;


    public static class Input
    {
        public static int[] GetScriptParamInts(this IEngine engine, string param)
        {
            var paramRaw = engine.GetScriptParam(param)?.Value;
            if (string.IsNullOrWhiteSpace(paramRaw))
                throw new ArgumentNullException(param);

            try
            {
                // first try as json structure (from low code app)
                // eg "["123"]"
                return JsonConvert
                    .DeserializeObject<string[]>(paramRaw)
                    .Select(int.Parse)
                    .ToArray();
            }
            catch (JsonSerializationException)
            {
                // not valid json, try parse as normal input parameters
                // eg "789"
                return paramRaw
                    .Replace(" ", string.Empty) // remove spaces
                    .Split(',')
                    .Select(int.Parse)
                    .ToArray();
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to parse {param}: " + ex.Message);
            }
        }
    }
}
