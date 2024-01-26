def correct_format_date(date: str)-> bool:
    """
    Corret date should be in the form of yyyy-mm-dd
    """
    # TODO
    return True

def correct_data_collections(data_collection:str)-> bool:
    """
    Check if the data_collection inserted is a valid collection
    """
    # TODO
    return True

def valid_aoi(aoi)-> bool:
    """
    Check if the aoi (geometry) inserted is a valid geometry 
    """
    # TODO
    return True

def valid_order_by(order_by: str)->bool:
    """"
    Check if the order_by string is a valid parameter
    """
    if order_by!= None:
        if  order_by =='asc' or order_by == 'desc':
            return True
    return False

def valid_product_type(type_: str)-> bool:
    """"
    TODO
    Check if the product_type string is a valid parameter
    """
    return True
    # if type_ == "SLC" or type_ == "FRD" or type_ == "OCN":
    #     return True
    # return False

def valid_mode(mode: str)-> bool:
    """"
    TODO
    Check if the mode string is a valid parameter
    """
    return True
    # if mode =='IW':
    #     return True
    # return False

def valid_direction(direction: str)-> bool:
    """"
    Check if the direction string is a valid parameter
    """
    # TODO
    return True

def valid_orbit(orbit: int)-> bool:
    """"
    TODO
    Check if the orbit string is a valid parameter
    """
    return True
    # if orbit>=0 and orbit<=1000000000:
    #     return True
    # return False

def valid_plevel(plevel: str)-> bool:
    """"
    Check if the tileId string is a valid parameter
    """
    # TODO
    return True
    # if plevel == 'S2MSI2A':
    #     return True
    # return False

def valid_tileId(tile: str)-> bool:
    """"
    Check if the tileId string is a valid parameter
    """
    # TODO
    return True