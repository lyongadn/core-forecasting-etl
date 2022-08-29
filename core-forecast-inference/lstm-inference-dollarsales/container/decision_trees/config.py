import json

class Config():
    def __init__(self, filename):
        self.config = json.load(open(filename, 'r'))
        self.param_names = []
        self.all_params = {}
        for param_head in self.config.keys():
            for param in self.config[param_head].keys():
                self.param_names.append(param)
                self.all_params[param] = self.config[param_head][param]

    def get_param_names(self):
        return self.param_names
        
    def get_params(self):
        return self.all_params
    
    def get_param(self, param):
        return self.all_params[param]

    def get_location(self, param, replace_list = ""):
        if isinstance(self.all_params[param], list):
            loc = self.combine_path(self.all_params[param])
        else:
            loc = self.all_params[param]
        if len(replace_list) == 1:
            return loc.replace("--", replace_list[0])
        elif len(replace_list) > 1:
            new_str = ""
            for i in range(len(replace_list)):
                if i == 0:
                    new_str += loc.replace("--", replace_list[i], 1)
                else:
                    new_str = new_str.replace("--", replace_list[i], 1)
            return new_str
        elif len(replace_list) < 1:
            return loc

    def combine_path(self, path_arr):
        return "/".join(path_arr)

#    def get_path(self, path_key, metric, location_num, product=0):
#        if metric=="sales_sub_total":
            

