package map_reduce.Utils;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ParserJson {


    public static String parser(String jsonString, String name) throws JSONException {
        JSONObject jsonObject = new JSONObject(jsonString);


        return name.equals("brand")?
                removeUnnecessaryCharacters(jsonObject.getString(name)):
                jsonObject.getString(name);
    }

    private static String removeUnnecessaryCharacters(String string){
        if(string.equals("")){
            return "Unknown";
        }
        String[] items = string.split("\n");
        if(items.length == 1){
            return items[0];
        }
        return items[2].trim();
    }
}