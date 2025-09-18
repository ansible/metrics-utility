


class Event_Rollups:
    """
    Event rollups operate over main_jobevent collector data
    """
    def base(dataframe):
        """
        This function will create first level aggregation of the event dataframe, the result is json
        
        Aggregations are:
            - Avg number of modules used in a playbook
            - Failure/Success rate of modules
            - Modules Used to Automate
            - Total number of modules automated
        """
        json_data = {}

        # Avg number of modules used in a playbook
        json_data['avg_modules_used'] = dataframe['modules_used'].mean()

        # Failure/Success rate for each module
        
        
        

        return json_data


    def aggregate(json_data):
        """
        This function will create rollups from aggregations, it should accept json (result of base function) 
        and should produce the same jsob, but with aggregated values, so I can create monthly and yearly rollups
        """
