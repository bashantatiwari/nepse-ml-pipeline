def getStatus(open_price, close_price):
    if close_price > open_price:
        return "up"
    elif close_price < open_price:
        return "down"
    return "same"
