from typing import TypedDict
from datetime import date

class Ballpark(TypedDict):
    _id: int
    name: str
    city: str
    state: str

class Team(TypedDict):
    _id: int
    name: str
    city: str
    first: date
    last: date

class Player(TypedDict):
    _id: int
    firstname: str
    lastname: str
    dob: date
    bats: str
    throws: str

class Start(TypedDict):
    player: int
    team: int
    battingPos: int
    fieldingPos: int

class Sub(TypedDict):
    player: int
    team: int
    battingPos: int
    fieldingPos: int
    inning: int
    pinchHit: int
    pinchRun: int

class AtBat(TypedDict):
    number: int
    batter: int
    inning: int
    top_bottom: str
    pitches: str
    play: str
    playDetails: str
    baserunnerDetails: str

class Game(TypedDict):
    _id: int
    hometeam: int
    visteam: int
    date: date
    location: int
    usedh: bool
    htbf: bool
    attendance: int
    winningpitcher: int
    losingpitcher: int
    save: int
    starts: list[Start]
    subs: list[Sub]
    atBats: list[AtBat]