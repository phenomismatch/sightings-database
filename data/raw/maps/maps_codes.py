"""Create codes data."""

import csv

lhold = {
    'A': 'United States Department of Agriculture',
    'AF': 'Forest Service',
    'D': 'United States Department of Defense',
    'DA': 'Army',
    'DC': 'Coast Guard',
    'DE': ("United States Department of Energy "
           "(grouped with DoD as per Phil's request)"),
    'DG': 'National Guard',
    'DM': 'Marine',
    'DN': 'Navy',
    'DO': 'Air Force',
    'DX': 'Army Corps of Engineers',
    'I': 'United States Department of the Interior',
    'IB': 'Bureau of Land Management',
    'IG': 'Geological Survey',
    'II': 'Bureau of Indian Affairs',
    'IP': 'National Park Service (NP or NRA)',
    'IR': 'Bureau of Reclamation',
    'IW': 'Fish and Wildlife Service (NWR)',
    'O': 'United States Department of Commerce',
    'N': 'National Oceanic and Atmospheric Administration (NOAA)',
    'V': 'Environmental Protection Agency',
    'S': 'State government',
    'C': 'City or county government',
    'P': 'Private',
    'F': 'Non-American land holding',
    'FC': 'city or county',
    'FD': 'Department of Defense/military',
    'FF': 'forest service',
    'FP': 'national park',
    'FR': 'private',
    'FS': 'state or province',
    'FW': 'fish and wildlife service',
    'F-': 'landholder unknown'}

holdcert = {
    '+': ('The determination is certain and confirmed with the '
          'operator or current GIS map'),
    ' ': 'blank. There is some uncertainty in the determination',
    '?': 'there is great uncertainty in the determination'}

o = {
    'A': ('agency station. Operated by The Institute for Bird Populations '
          '(IBP) personnel using IBP bands'),
    'I': ('independent station. Has always been operated by non-IBP '
          'personnel; do not use IBP bands.'),
    'D': ('previously operated by IBP personnel (usually on DOD lands) '
          'but operations have been taken over by non-IBP personnel '
          'and do not use IBP bands'),
    'B': ('previously operated by IBP personnel but operations have been '
          'taken over by non-IBP personnel. Uses IBP bands for operations, '
          'therefore, IBP is responsible for scheduling and insuring '
          'permitting requirements are met.'),
    'S': ('has always been operated by non-IBP personnel but uses IBP bands. '
          'IBP is responsible for scheduling and insuring permitting '
          'requirements are met.')}

precision = {
    'BLK': '10-minute block',
    '10M': '10 minutes',
    '01M': '01 minute',
    '10S': '10 seconds',
    '05S': '5 seconds',
    '01S': '1 second',
    '-': 'no latitude nor longitude information available for this station'}

source = {
    'GIS': 'GIS program (can include ArcView, ArcMAP, etc)',
    'GPS': 'hand held GPS unit',
    'Web': ('Web based mapping program (can include topozone.com, '
            'Google Earth, etc)'),
    'hard': ('hard copy map (can include USGS topographic map, '
             'county map, etc.)')}

datum = {
    'NAD27': ('North American Datum of 1927 is a datum based on the '
              'Clarke ellipsoid of 1866'),
    'NAD83': 'North American Datum of 1983 is an earth-centered datum based '
             'on the Geodetic Reference System of 1980. Considered '
             'equivalent to the WGS84 datum for this database.',
    '-': ('(dash) confirmed datum information not available for these '
          'coordinates.')}

passed: {
    ' ': ('no comparison of the operator\'s MAPSPROG verified data has been '
          'made against data verified by IBP personnel or the operator does '
          'not use MAPSPROG.'),
    'Y': ('a comparison has been performed and the operator\'s MAPSPROG data '
          'was very comparable to that verified by IBP personnel; yr= most '
          'recent data year upon which the comparison was performed.'),
    'N': ('a comparison was performed but the operator\'s final MAPSPROG '
          'data had several important differences from the final files '
          'created during verification by IBP personnel; yr= most recent data '
          'year upon which the comparison was performed.')}

map_ = {
    'X': ('current map of station showing habitat types and net sites is '
          'confirmed to be on file'),
    'x': ('current map of station showing habitat types and net sites is '
          'believed to be on file but it has not been confirmed that it can '
          'be used for current GIS analyses'),
    '.': ('no current map is present and has been confirmed by examining the '
          'hard copy files'),
    ' ': ('no current map believed to be present but this needs to be '
          'confirmed by examining the hard copy files')}

dyr_yyr = {
    'D': 'The following codes can be used in any of the D<yr> fields',
    'DX': 'banding data received for the year',
    'DH': ('unable to acquire the data from the station operator, but the '
           'station was operated in the year'),
    'DL': ('data lost by the station operator, but the station was operated '
           'in the year'),
    'DP': ('only partial data available. Not enough for analysis, e.g. no '
           'recapture records provided by operator.'),
    'Y': 'The following codes can be used in any of the Y<yr> fields',
    'YX': 'station believed to have been operated in the year',
    'Y?': 'uncertain if station was operated in the year'}

d89 = {
    'N': 'the station was not run that year',
    'I': 'the station was operated but the data has not yet been verified',
    'S': ('the station met the requirements for use in survivorship analyses. '
          'The station ran three complete periods within the adult '
          'superperiod A complete period must have run at least 1/3 the '
          'normal effort per period. Normal effort is defined by the '
          'standard open and close'),
    'T': ('station did not quite meet the requirements for survivorship '
          'analysis as outlined above for S, but it was decided '
          'administratively that the effort was close enough for the year to '
          'be considered usable for survivorship analysis'),
    'B': ('he data from the year meets the criteria for both survivorship and '
          'productivity analyses. To be used in productivity analyses the '
          'station must usable for survivorship and, in addition, must have '
          'run a minimum of two complete periods in the young superperiod '
          '(see above for the definition of a complete period.) The total '
          'effort included in the young superperiod must be at least 1⁄2 the '
          'normal effort of two periods.'),
    'C': ('station did not quite meet the requirements for usability in both '
          'survivorship and productivity analysis as outlined above for B, '
          'but it was decided administratively that the effort was close '
          'enough for the year to be considered usable for both survivorship '
          'and productivity analysis'),
    'X': ('the station was operated but the data from the year meets the '
          'criteria for neither survivorship nor productivity analyses')}

bs = {
    'band size': 'New captures',
    'R': 'Recaptures',
    'U': 'Unbanded birds'}

c = {
    'N': 'newly banded bird',
    'R': 'recaptured bird',
    'U': 'unbanded bird',
    'L': 'lost band',
    'D': 'destroyed band',
    'C': ('changed band (duplicate recapture record containing the '
          'original band number)'),
    'A': 'added band (double-banded bird)'}

numb = {
    '11475': "Traill's Flycatcher (includes Alder, Willow, and Traill’s)",
    '11555': ('Western Flycatcher (includes Pacific-slope, Cordilleran, '
              'and Western)')}

age = {
    '0': 'indeterminable age',
    '4': 'local (young bird incapable of sustained flight)',
    '2': 'hatching-year bird',
    '1': 'after-hatching-year bird',
    '5': 'second-year bird',
    '6': 'after-second-year bird',
    '7': 'third-year bird',
    '8': 'after-third-year bird'}


ha = {
    'S': 'skull pneumaticization',
    'B': 'brood patch',
    'C': 'cloacal protuberance',
    'L': ('presence of two generations of feathers within a feather tract or '
          'between two adjacent feather tracts'),
    'P': 'plumage (exact plumage not specified)',
    'A': 'adult plumage',
    'H': '1st basic plumage',
    'J': 'juvenal plumage',
    'E': 'eye color',
    'F': 'flight feather wear',
    'M': 'active molt occurring',
    'I': 'mouth/bill',
    'O': 'other (needs explanation in notes)',
    'R': 'recapture information from between-record verification',
    'V': ('photo verification done and age changed based upon information '
          'in photo'),
    'G': ('used by IBP after changing the age to a less specific age category '
          'because it is not possible to age the bird to the more specific '
          'category in that month according to Pyle (1997)'),
    'U': ('used by IBP when HA is not provided or cannot be assessed from '
          'supplemental data'),
    'Y': ('used by IBP after changing the age to 1 (AHY) because no useful '
          'evidence exists within the Molt Limits and Plumage fields to '
          'support the more specific age of 5, 6, 7 or 8 (SY, ASY, TY or ATY) '
          'in that month according to Pyle (1997)')}

wrp = {
    'UCU': 'unknown molt cycle, unknown plumage',
    'UCB': 'unknown molt cycle, basic plumage',
    'UCA': 'unknown molt cycle, alternate plumage',
    'FCJ': 'first molt cycle, juvenal plumage',
    'FCF': 'first molt cycle, formative plumage',
    'FCA': 'first molt cycle, alternate plumage',
    'FCS': 'first molt cycle, supplemental plumage',
    'SCB': 'second molt cycle, basic plumage',
    'SCA': 'second molt cycle, alternate plumage',
    'DCB': 'definitive molt cycle, basic plumage',
    'DCA': 'definitive molt cycle, alternate plumage'}

sex = {
    'M': 'male',
    'F': 'female',
    'U': 'unknown',
    'X': 'unattempted'}

hs = {
    'B': 'brood patch',
    'C': 'cloacal protuberance',
    'P': 'plumage',
    'J': 'juvenal plumage',
    'E': 'eye color',
    'I': 'mouth/bill',
    'O': 'other (requires explanation in notes)',
    'T': 'tail length',
    'W': 'wing chord',
    'R': 'recapture information (based on between-record verification)',
    'G': ('used by IBP after changing the sex to unknown because it is not '
          'possible to sex the bird male or female in that month according '
          'to Pyle (1997)'),
    'U': ('used by IBP when HS is not provided or cannot be assessed from '
          'supplemental data')}

sk = {
    '0': 'none',
    '1': 'trace (less than 5%)',
    '2': 'less than 1/3 but greater than 5%',
    '3': 'half (1/3 to 2/3)',
    '4': 'greater than 2/3 but less than 95%',
    '5': 'almost complete (greater than 95%)',
    '6': 'complete',
    '8': 'undeterminable, but attempted'}

cp = {
    '0': 'none',
    '1': 'small',
    '2': 'medium',
    '3': 'large'}

bp = {
    '0': 'none',
    '1': 'smooth (feathers lost)',
    '2': 'vascularized',
    '3': 'heavy (very heavily vascularized)',
    '4': 'wrinkled',
    '5': 'molting (growing new feathers)'}

f = {
    '0': 'none',
    '1': 'trace (furculum less than 5% filled)',
    '2': 'light (furculum greater than 5% but less than 1/3 filled)',
    '3': 'half (furculum 1/3 to 2/3 filled)',
    '4': 'full (furculum greater than 2/3 filled but not bulging)',
    '5': 'bulging',
    '6': 'greatly bulging',
    '7': 'very excessive'}

bm = {
    '0': 'none',
    '1': 'trace',
    '2': 'light',
    '3': 'medium',
    '4': 'heavy'}

fm = {
    'N': 'no flight feather molt',
    'A': 'asymmetric',
    'S': 'symmetric',
    'J': 'juvenal flight feather growth'}

fw = {
    '0': 'none',
    '1': 'slight',
    '2': 'light',
    '3': 'moderate',
    '4': 'heavy',
    '5': 'excessive'}

jp = {
    '3': 'full juvenal plumage',
    '2': 'greater than 1⁄2 juvenal plumage but not full',
    '1': 'less than 1⁄2 juvenal plumage but some remaining',
    '0': 'none, completely molted into basic plumage'}

status = {
    '000': 'not banded or bird died prior to release',
    '300': 'healthy bird banded and released',
    '301': 'healthy bird color-banded and released',
    '325': 'healthy bird with geotracker and released',
    '500': 'injured bird banded and released',
    '501': 'injured bird color-banded and released'}

disp = {
    'O': 'old (healed) injury',
    'M': 'malformed (deformity such as crossed mandibles)',
    'W': 'wing injury',
    'L': 'leg injury',
    'T': 'tongue injury',
    'E': 'eye injury',
    'B': 'body injury',
    'I': 'illness/infection/disease',
    'S': 'stress or shock',
    'P': 'predation (death due to predation)',
    'D': ('dead (death due to causes other than predation or removed '
          'permanently from station)'),
    'R': 'band removed from bird and then bird released bandless',
    " ": 'blank, bird released alive, uninjured'}

note = {
    'NM': 'not MAPS: record not from a MAPS station or a MAPS net',
    " ": 'blank, no note43.'}

ppc = {
    'J': ('Juvenal; feather tract comprised entirely of retained juvenal '
          'feathers or non-feathered body part shows characteristics '
          'indicative of a young bird'),
    'L': 'Molt limit; molt limit between juvenal and formative feathers',
    'F': 'Formative; feather tract comprised entirely of formative feathers',
    'B': ('Basic; feather tract entirely of basic feathers or non-feathered '
          'body part shows characteristics indicative of an adult bird'),
    'R': ('Retained; both juvenal and basic feathers are present within '
          'the tract'),
    'M': ('Mixed; multiple generations of basic feathers are present in '
          'the tract'),
    'A': 'Alternate; ALL feathers in the tract are of alternate plumage',
    'N': ('Non-juvenal; feather tract may include formative, basic, and/or '
          'alternate feathers, but no juvenal feathers are present.'),
    'U': ('Unknown; feather tract or non-feathered body part examined, but '
          'shows ambiguous characteristics or cannot be coded with '
          'confidence'),
    '1': 'tract is not indicative of a specific adult age class',
    '5': ('tract contains some or all retained juvenal feathers, indicating '
          'a second-year bird'),
    '6': ('tract contains no retained juvenal feathers (or few juvenal '
          'feathers in non-passerines), indicating an after-second-year bird'),
    '7': ('tract contains few retained juvenal feathers, indicating a '
          'third-year bird'),
    '8': ('tract contains no retained juvenal feathers, indicating an '
          'after-third-year bird')}

nf = {
    'J': 'non-feather parts indicative of a young bird',
    'B': 'non-feather parts indicative of an adult bird',
    'N': 'non-feather parts indicative of an adult bird',
    'U': 'Unknown; non-feather parts not indicative of a specific age',
    '1': 'non-feather parts not indicative of a specific adult age class',
    '5': ('non-feather parts show some retained juvenal characteristics, '
          'indicating a second-year bird'),
    '6': ('non-feather parts show no retained juvenal characteristics, '
          'indicating an after-second-year bird')}

fp = {
    'P': ('rectrices pulled (possibly contour feathers as well) '
          '(pre-2006 coding)'),
    'C': 'only contour feathers pulled (pre-2006 coding)',
    'O': ('Outer two rectrices were pulled (i.e., rectrix 6 from both the '
          'left and right side of the tail). Previous to 2006 this was '
          'indicated by FTHR. PULL = P.'),
    'I': ('An inner and an outer rectrix were pulled (i.e., rectrix 1 '
          'from one side and rectrix 6 from the other side were pulled).'),
    "-": 'dash; no feathers were pulled'}

sw = {
    '1': ('1mm wide swab used to collect the sample from within the '
          'cloacal cavity'),
    '2': ('2mm wide swab used to collect the sample from within the '
          'cloacal cavity'),
    'Y': ('swab used to collect the sample from within the cloacal '
          'cavity but of unknown size'),
    "-": 'dash; no swab sample was taken'}

sc = {
    'U': 'skull suggests age unknown, but age determined',
    'Y': 'skull suggests HY bird, but AGE not equal to 2 or 4',
    'A': 'skull suggests adult bird, but AGE not equal to 1, 5 or 6',
    '5': 'SK=5, record re-examined',
    " ": 'blank, record OK, not re-examined'}

cc = {
    'A': 'CP suggests adult, but AGE not equal to 1, 5 or 6',
    'M': 'CP suggests male, but SEX not equal to M',
    'U': 'SEX=M, but CP is blank',
    '1': 'CP=1, record re-examined',
    'H': 'AGE=0, 2 or 4, but SEX=M',
    'P': 'SEX=M, but CP=0',
    " ": 'blank, record OK, not re-examined'}

bc = {
    'A': 'BP suggests adult, but AGE not equal to 1, 5 or 6',
    'F': 'BP suggests female, but SEX not equal to F',
    'U': ('Pre-1997: SEX=F, but BP=" " or BP#3 in species in which males '
          'develop BPs :: 1997+: only used when SPEC=WREN and SEX=F; '
          'SEX should probably be U'),
    '5': 'BM>2 and BP=5, record re-examined',
    'H': 'AGE=0, 2, or 4, but SEX=F',
    'P': 'SEX=F, but BP=0',
    '1': 'BP=1 or 5, record re-examined',
    " ": 'blank, record OK, not re-examined'}

mc = {
    'A': 'FM suggests adult, but AGE not equal to 1, 5 or 6',
    'Y': 'BM+FM suggest HY, but AGE not equal to 2 or 4',
    " ": 'blank, record OK, not re-examined'}

wc = {
    'A': 'FW suggests adult, but AGE not equal to 1, 5 or 6',
    " ": 'blank, record OK, not re-examined'}

jc = {
    'Y': 'JP suggests HY, but AGE not equal to 2 or 4',
    " ": 'blank, record OK, not re-examined'}

v1 = {
    '2': ('two records with C=N and the same band number or two records with '
          'C=R and the same date, time and net'),
    'BN': 'band number discrepancy',
    'SP': 'species discrepancy',
    'NM': 'species sequence number discrepancy',
    'A': 'age discrepancy',
    'WP': 'WRP discrepancy',
    'S': 'sex discrepancy',
    'DL': 'destroyed/lost band and a captured bird with the same band number',
    'ST': 'station discrepancy',
    'SS': 'status discrepancy',
    " ": 'blank, record OK, not re-examined'}

n_band = {
    'O': 'not caught at MAPS station',
    'S': 'caught within MAPS station boundary, but not in a MAPS net',
    'E': 'part of extremely irregular effort at site',
    'D': 'date outside of MAPS periods',
    'T': 'time outside normal MAPS operation for that station',
    '?': 'uncertain species identification or band number',
    'H': 'hummingbird',
    'G': 'gallinaceous bird',
    'U': 'unbanded bird released alive',
    'R': 'recaptured bird, but no band number recorded',
    '-': 'record examined with current MAPS analytical procedures',
    '+': ('record examined with preliminary MAPS analytical procedures '
          '(1989-1991)')}

n_effort = {
    'O': 'effort from net not within the MAPS station boundary',
    'S': 'effort from net within MAPS station boundary, but not a MAPS net',
    'E': 'part of extremely irregular effort at site',
    'D': 'date outside of MAPS periods, but a MAPS net',
    'T': ('time outside normal MAPS operation for that station for that year, '
          'but a MAPS net and during the MAPS season16.')}


b_band = {
    'B': ('non-comparable, using net-by-net, hour-by-hour protocol '
          '(protocol used subsequent to 1991)'),
    'Y': ('non-comparable using net-by-net, period-by-period protocol '
          '(one protocol used prior to 1992)'),
    'X': ('non-comparable using period-by-period protocol (another '
          'protocol used prior to 1992)'),
    '-': 'comparable by B or M protocol',
    '+': 'comparable by Y or X protocol',
    '*': ('no comparison made; constant-effort analyses not completed '
          'between this year of operation and the preceding year '
          'of operation.')}

a = {
    'A': ('(takes place of B) non-comparable using net-by-net, hour-by-hour '
          'protocol'),
    '*': ('no comparison made; constant-effort analyses not completed '
          'between this year of operation and the following year of '
          'operation Structure for 2016 MAPS banding data')}

ip = {
    'Period 1': 'May 01 - May 10',
    'Period 2': 'May 11 - May 20',
    'Period 3': 'May 21 - May 30',
    'Period 4': 'May 31 - June 09',
    'Period 5': 'June 10 - June 19',
    'Period 6': 'June 20 - June 29',
    'Period 7': 'June 30 - July 09',
    'Period 8': 'July 10 - July 19',
    'Period 9': 'July 20 - July 29',
    'Period 10': 'July 30 - August 08',
    'Period 11': ('August 09 - August 18 (only part of regular MAPS effort '
                  'up through the 1996 MAPS season)'),
    'Period 12': ('August 19 - August 28 (only part of regular MAPS effort '
                  'up through the 1996 MAPS season)'),
    'Period 13': 'August 29 – September 07',
    'Period 14': 'September 08 – September 17',
    'Period 15': 'September 18 - September 27',
    'Period 16': 'September 28 - October 07',
    'Period 17': 'October 08 - October 27',
    'Period 18': 'October 28 – November 01',
    'Period 19': 'November 02 - November 11',
    'Period 20': 'November 12 - November 21',
    'Period 21': 'November 22 – December 01',
    'Period 22': 'December 02 - December 11',
    'Period 23': 'December 12 - December 21',
    'Period 24': 'December 22 - December 31',
    'Period 25': 'January 01 - January 10',
    'Period 26': 'January 11 - January 20',
    'Period 27': 'January 21 - January 30',
    'Period 28': 'January 31 – February 09',
    'Period 29': 'February 10 - February 19',
    'Period 30': 'February 20 - March 01',
    'Period 31': 'March 02 - March 11',
    'Period 32': 'March 12 - March 21',
    'Period 33': 'March 22 – March 31',
    'Period 34': 'April 01 - April 10',
    'Period 35': 'April 11 - April 20',
    'Period 36': 'April 21 - April 30',
    "88": 'non-MAPS effort (use for MAPS data but not for TMAPS)'}

man = {
    'B': ('broken effort. Effort for a net on one day where the hours of '
          'effort were broken into two or more time blocks. It involves both '
          'start1 and 2 and end1 and 2. (i.e. 060-072, 091-115)'),
    '#': ('divided effort. Effort for a net on multiple days (the number of '
          'days are entered into the field, i.e. 2, 3, etc.) required to '
          'make up the full effort for that period and sub-period. '
          '(e.g. May 05 060-090, May 06 090-120, Man=2)'),
    '?': ('designates that ANET, START, or END lack full information and '
          'were compiled based upon the data available '
          '(usually a result of the protocol up to 1992).')}

ps1 = {
    'C': ('confirmed breeder; information found during this period confirms '
          'the species as a breeder for the season'),
    'P': ('probable breeder; information found during this period suggests, '
          'but does not confirm a species as a breeder:'),
    'O': ('observed; information found during this period indicates the '
          'species was detected, but displayed no evidence of local breeding'),
    '-': ('absent; the species was not encountered during this period')}

sb1 = {
    'N': ("current year's nest found in the study area with eggs or young, "
          "in the process of being built, or already depredated or abandoned"),
    'M': ('adult seen gathering or carrying nesting material to a likely '
          'nest site in the study area'),
    'F': ('adult seen carrying food or fecal sac to or from a likely nest '
          'site in the study area'),
    'D': ('distraction display or injury feigning by an adult bird'),
    'L': ('a young bird incapable of sustained flight (a "local") '
          'captured in the study area; or very young (stub-tailed) '
          'fledglings found being fed by parents in the study area'),
    'C': ('copulation or courtship observed of a species within its '
          'breeding range'),
    'T': ('other territorial behavior observed in the study area'),
    'S': ('territorial song or drumming heard'),
    'B': ('bird captured or banded. NOTE: The presence of a brood patch or '
          'cloacal protruberance on a single individual is not valid '
          'evidence of local breeding'),
    'E': ('bird encountered (seen or heard) in the study area but with no '
          'territorial behavior'),
    'O': ('bird encountered flying over the study area.'),
    'Z': ('bird both captured/banded and encountered in, or flying over, '
          'the study area.')}

ys = {
    'B': ('breeder (at least one individual was a summer resident at the '
          'station)'),
    'L': ('likely breeder (at least one individual was a suspected summer '
          'resident at the station)'),
    'T': ('transient (station is within the breeding range of the species, '
          'but no individual of the species was a summer resident at '
          'the station)'),
    'A': ('altitudinal disperser (species breeds only at lower elevations '
          'than that of the station and which disperses to higher '
          'elevations after breeding)'),
    'H': ('high altitudinal disperser (species breeds usually designated an '
          'altitudinal disperser. However, has resided during the height of '
          'the breeding season (not just during the post-breeding period) in '
          'a given year above its normal breeding elevation.'),
    'M': ('migrant (station is not within the breeding range of the species, '
          'and the species was not a summer resident)'),
    'E': ('extralimital breeder (one or more individuals of the species was '
          'a summer resident at the station, but the station lies outside of '
          'the normal breeding range of the species) - absent (no evidence '
          'of species in data; presumably absent from station during year '
          'in question)'),
    '?': ('uncertain species identification or band number (no breeding '
          'status assigned)'),
    '#': ('station operated this year, but breeding status determinations '
          'were not made for species that were not captured; used only for '
          'species without capture records'),
    'D': ('the species was only encountered at the station outside of the '
          'MAPS season, but the station lies within breeding range of the '
          'species.'),
    'W': ('the species was only encountered at the station outside of the '
          'MAPS season, and the station lies outside of the breeding '
          'range of species.'),
    '@': ('the Breeding Status List is missing or incomplete for these '
          'species this year.')}

b_status = {
    'X': 'species was captured',
    'R': ('species was not captured, but breeding status information was '
          'recorded on a breeding status list, a point count form, etc.'),
    " ": ('blank, species was not capturedStructure for 2016 MAPS '
          'Breeding Status Data')}

tables = {
    'a': a,
    'age': age,
    'b_band': b_band,
    'b_status': b_status,
    'bc': bc,
    'bm': bm,
    'bp': bp,
    'bs': bs,
    'c': c,
    'cc': cc,
    'cp': cp,
    'd89': d89,
    'datum': datum,
    'disp': disp,
    'dyr_yyr': dyr_yyr,
    'f': f,
    'fm': fm,
    'fp': fp,
    'fw': fw,
    'ha': ha,
    'holdcert': holdcert,
    'hs': hs,
    'ip': ip,
    'jc': jc,
    'jp': jp,
    'lhold': lhold,
    'man': man,
    'map': map_,
    'mc': mc,
    'n_band': n_band,
    'n_effort': n_effort,
    'nf': nf,
    'note': note,
    'numb': numb,
    'o': o,
    'ppc': ppc,
    'precision': precision,
    'ps1': ps1,
    'sb1': sb1,
    'sc': sc,
    'sex': sex,
    'sk': sk,
    'source': source,
    'status': status,
    'sw': sw,
    'v1': v1,
    'wc': wc,
    'wrp': wrp,
    'ys': ys}


with open('data/raw/maps/maps_codes.csv', 'w') as csv_out:
    writer = csv.writer(csv_out)
    writer.writerow(['field', 'code', 'value'])
    for field, table in tables.items():
        for code, value in table.items():
            writer.writerow([field, code, value])
