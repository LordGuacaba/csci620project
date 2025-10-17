class Relation:

    def __init__(self, name, cols: tuple[str]):
        self.name = name
        self.cols = cols
        self.values = {colName: None for colName in cols}

    def getValues(self):
        return [self.values[colName] for colName in self.cols]
    
    def setValue(self, col: str, value):
        self.values[col] = value
    
class Game(Relation):

    def __init__(self):
        super().__init__("Games", ("id", "hometeam", "visteam", "date", "location", "usedh", "htbf", "attendance", "winningpitcher", "losingpitcher", "sv"))

class Ballpark(Relation):

    def __init__(self):
        super().__init__("Ballparks", ("id", "name", "city", "state"))

class Team(Relation):

    def __init__(self):
        super().__init__("Teams", ("id", "city", "name", "first", "last"))

class Player(Relation):

    def __init__(self):
        super().init("Players", ("id", "firstname", "lastname", "DOB", "bats", "throws"))

class PlayerActivity(Relation):

    def __init__(self):
        super().__init__("PlayerActivity", ("id", "gameid", "playerid", "team", "battingPos", "fieldingPos", "pinchHit", "pinchRun"))

class AtBat(Relation):

    def __init__(self):
        super().__init__("AtBats", ("num", "game", "batter", "inning", "top_bottom", "pitches", "play", "playdetails", "baserunnerdetails"))