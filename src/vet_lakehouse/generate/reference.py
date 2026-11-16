"""Static reference values used by the synthetic data generator.

Everything here is invented. The clinic group, people and pets do not exist.
"""

from __future__ import annotations

from dataclasses import dataclass

GROUP_NAME = "Lumipaw"  # fictional veterinary group


@dataclass(frozen=True)
class Country:
    """A country the fictional group operates in."""

    code: str
    name: str
    currency: str
    cities: tuple[str, ...]
    eur_factor: float  # rough local-currency units per 1 EUR, used to make realistic prices
    price_level: float  # relative price level compared to the euro-zone clinics
    phone_prefix: str


COUNTRIES: tuple[Country, ...] = (
    Country("BE", "Belgium", "EUR", ("Antwerp", "Ghent", "Leuven"), 1.0, 1.0, "+32"),
    Country("NL", "Netherlands", "EUR", ("Utrecht", "Rotterdam", "Eindhoven"), 1.0, 1.05, "+31"),
    Country("DE", "Germany", "EUR", ("Cologne", "Hamburg", "Munich"), 1.0, 1.1, "+49"),
    Country("SE", "Sweden", "SEK", ("Stockholm", "Gothenburg"), 11.3, 1.2, "+46"),
    Country("RS", "Serbia", "RSD", ("Belgrade", "Novi Sad", "Nis"), 117.0, 0.55, "+381"),
)

FIRST_NAMES = (
    "Anna", "Bram", "Clara", "Daan", "Elsa", "Finn", "Greta", "Hugo", "Ines", "Jonas",
    "Katja", "Lars", "Mila", "Nikola", "Olga", "Pieter", "Rosa", "Stefan", "Tessa", "Uros",
    "Vera", "Wout", "Yara", "Zoran", "Emma", "Luka", "Sofia", "Milos", "Freja", "Jelena",
)  # fmt: skip

LAST_NAMES = (
    "Peeters", "Janssens", "de Vries", "Bakker", "Schmidt", "Weber", "Lindqvist", "Berg",
    "Jovanovic", "Petrovic", "Maes", "Visser", "Fischer", "Nilsson", "Nikolic", "Claes",
    "Smit", "Wagner", "Holm", "Ilic", "Dubois", "Mulder", "Keller", "Sandberg", "Markovic",
)  # fmt: skip

STREETS = (
    "Station Road", "Market Street", "Church Lane", "Park Avenue", "Mill Street",
    "Harbour Street", "Linden Alley", "Garden Row", "River Walk", "School Street",
)  # fmt: skip

PET_NAMES = (
    "Luna", "Max", "Bella", "Charlie", "Milo", "Nala", "Rocky", "Coco", "Simba", "Lola",
    "Buddy", "Kira", "Oscar", "Mimi", "Rex", "Zoe", "Toby", "Lucky", "Maja", "Felix",
    "Bruno", "Daisy", "Leo", "Nina", "Jack", "Cleo", "Sam", "Pixie", "Bobi", "Sara",
)  # fmt: skip

# species -> (weight for random choice, breeds, min weight kg, max weight kg)
SPECIES: dict[str, tuple[int, tuple[str, ...], float, float]] = {
    "dog": (
        55,
        ("Labrador", "German Shepherd", "Beagle", "Poodle", "Mixed breed", "Border Collie"),
        4.0,
        45.0,
    ),
    "cat": (35, ("European Shorthair", "Maine Coon", "Siamese", "Mixed breed"), 2.5, 8.0),
    "rabbit": (6, ("Dutch", "Lop", "Lionhead"), 1.0, 4.0),
    "bird": (4, ("Budgerigar", "Canary", "Cockatiel"), 0.03, 0.4),
}

# The same species is written in several ways on purpose; Silver standardises them.
SPECIES_SPELLINGS: dict[str, tuple[str, ...]] = {
    "dog": ("dog", "Dog", " DOG ", "canine"),
    "cat": ("cat", "Cat", "CAT", "feline"),
    "rabbit": ("rabbit", "Rabbit"),
    "bird": ("bird", "Bird "),
}

VACCINES = ("Rabies", "Distemper", "Parvovirus", "Leptospirosis", "Feline leukemia", "Myxomatosis")

# reason -> (weight, min price EUR, max price EUR, typical duration in minutes)
REASONS: dict[str, tuple[int, float, float, int]] = {
    "checkup": (35, 40.0, 70.0, 20),
    "vaccination": (25, 35.0, 60.0, 15),
    "dental": (10, 120.0, 320.0, 45),
    "surgery": (8, 300.0, 1400.0, 90),
    "emergency": (7, 150.0, 900.0, 60),
    "grooming": (15, 30.0, 80.0, 40),
}

PAYMENT_METHODS = ("card", "cash", "bank_transfer", "insurance")
