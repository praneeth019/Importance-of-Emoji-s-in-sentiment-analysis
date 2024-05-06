import dask.dataframe as dd
import re
from dask.diagnostics import ProgressBar
import emoji

emoji_dict = {
    ":‑)": {"emoji": "🙂", "description": "happy face"},
    ":)": {"emoji": "🙂", "description": "happy face"},
    ":-]": {"emoji": "🙂", "description": " happy face"},
    ":]": {"emoji": "🙂", "description": " happy face"},
    ":->": {"emoji": "🙂", "description": " happy face"},
    ":>": {"emoji": "🙂", "description": " happy face"},
    "8-)": {"emoji": "🙂", "description": " happy face"},
    "8)": {"emoji": "🙂", "description": " happy face"},
    ":-}": {"emoji": "🙂", "description": " happy face"},
    ":}": {"emoji": "🙂", "description": " happy face"},
    ":^)": {"emoji": "🙂", "description": " happy face"},
    "=]": {"emoji": "🙂", "description": " happy face"},
    "=)": {"emoji": "🙂", "description": " happy face"},
    "☺️": {"emoji": "☺️", "description": " happy face"},
    ":‑D": {"emoji": "😃", "description": "Laughing"},
    ":D": {"emoji": "😃", "description": "Laughing"},
    "8‑D": {"emoji": "😎", "description": "grinning with glasses"},
    "8D": {"emoji": "😎", "description": "grinning with glasses"},
    "=D": {"emoji": "😄", "description": "big grin"},
    "=3": {"emoji": "😄", "description": "big grin"},
    "B^D": {"emoji": "😎", "description": "grinning with glasses"},
    "c:": {"emoji": "😃", "description": "Laughing"},
    "C:": {"emoji": "😃", "description": "Laughing"},
    "x‑D": {"emoji": "😆", "description": "Laughing"},
    "xD": {"emoji": "😆", "description": "Laughing"},
    "X‑D": {"emoji": "😆", "description": "Laughing"},
    "XD": {"emoji": "😆", "description": "Laughing"},
    ":-))": {"emoji": "😊", "description": "Very happy"},
    ":))": {"emoji": "😊", "description": "Very happy"},
    ":‑(": {"emoji": "☹️", "description": "sad"},
    ":(": {"emoji": "☹️", "description": "sad"},
    ":‑c": {"emoji": "☹️", "description": "pouting"},
    ":c": {"emoji": "☹️", "description": "pouting"},
    ":‑<": {"emoji": "☹️", "description": "pouting"},
    ":<": {"emoji": "☹️", "description": "pouting"},
    ":‑[": {"emoji": "☹️", "description": "sad"},
    ":[": {"emoji": "☹️", "description": "pouting"},
    ":-||": {"emoji": "☹️", "description": "sad"},
    ":{": {"emoji": "☹️", "description": "sad"},
    ":@": {"emoji": "☹️", "description": "pouting"},
    ":‑(": {"emoji": "☹️", "description": "sad"},
    ":'‑(": {"emoji": "😢", "description": "Crying"},
    ":'(": {"emoji": "😢", "description": "Crying"},
    ":=(": {"emoji": "😭", "description": "Crying"},
    ":'‑)": {"emoji": "🥹", "description": "Tears of happiness"},
    ":')": {"emoji": "🥹", "description": "Tears of happiness"},
    ':"D': {"emoji": "😂", "description": "Tears of happiness"},
    ">:(": {"emoji": "😠", "description": "Angry"},
    ">: [": {"emoji": "😠", "description": "Angry"},

    "D‑'": {"emoji": "😨", "description": "Horrory"},
    "D:<": {"emoji": "😨", "description": "Horror"},
    "D:": {"emoji": "😨", "description": "Horror"},
    "D8": {"emoji": "😱", "description": "shock"},
    "D;": {"emoji": "😨", "description": "Horror"},
    "D=": {"emoji": "😨", "description": "Horror"},
    "DX": {"emoji": "😩", "description": "great dismay"},
    
    ":‑O": {"emoji": "😮", "description": "Surprise, shock"},
    ":O": {"emoji": "😮", "description": "Surprise, shock"},
    ":‑o": {"emoji": "😮", "description": "Surprise, shock"},
    ":o": {"emoji": "😮", "description": "Surprise, shock"},
    ":-0": {"emoji": "😮", "description": "Surprise, shock"},
    ":0": {"emoji": "😮", "description": "Surprise, shock"},
    "8‑0": {"emoji": "😮", "description": "Surprise, shock"},
    ">:O": {"emoji": "😮", "description": "Surprise, shock"},
    "=O": {"emoji": "😮", "description": "Surprise, shock"},
    "=o": {"emoji": "😮", "description": "Surprise, shock"},
    "=0": {"emoji": "😮", "description": "Surprise, shock"},
    
    ":-3": {"emoji": "😺", "description": "Cat face"},
    ":3": {"emoji": "😺", "description": "Cat face"},
    "=3": {"emoji": "😺", "description": "Cat face"},
    "x3": {"emoji": "😺", "description": "Cat face"},
    "X3": {"emoji": "😺", "description": "Cat face"},
    
    ">:3": {"emoji": "😼", "description": "evil cat smile"},
    
    ":-*": {"emoji": "😗", "description": "Kiss"},
    ":*": {"emoji": "😙", "description": "Kiss"},
    ":x": {"emoji": "😚", "description": "Kiss"},
    
    ";‑)": {"emoji": "😉", "description": "Wink"},
    ";)": {"emoji": "😉", "description": "Wink"},
    "*-)": {"emoji": "😉", "description": "Wink"},
    "*)": {"emoji": "😉", "description": "Wink"},
    ";‑]": {"emoji": "😉", "description": "Wink"},
    ";]": {"emoji": "😉", "description": "Wink"},
    ";^)": {"emoji": "😉", "description": "Wink"},
    ";>": {"emoji": "😉", "description": "Wink"},
    ":‑,": {"emoji": "😉", "description": "Wink"},
    ";D": {"emoji": "😉", "description": "Wink"},
    ";3": {"emoji": "😉", "description": "Wink"},
   
    ":‑P": {"emoji": "😛", "description": "Tongue sticking out"},
    ":P": {"emoji": "😛", "description": "Tongue sticking out"},
    "X‑P": {"emoji": "😝", "description": "blowing a raspberry"},
    "XP": {"emoji": "😝", "description": "blowing a raspberry"},
    "x‑p": {"emoji": "😝", "description": "blowing a raspberry"},
    "xp": {"emoji": "😝", "description": "blowing a raspberry"},
    ":‑p": {"emoji": "😛", "description": "Tongue sticking out"},
    ":p": {"emoji": "😛", "description": "Tongue sticking out"},
    ":‑Þ": {"emoji": "😛", "description": "Tongue sticking out"},
    ":Þ": {"emoji": "😛", "description": "Tongue sticking out"},
    ":‑þ": {"emoji": "😛", "description": "Tongue sticking out"},
    ":þ": {"emoji": "😛", "description": "Tongue sticking out"},
    ":‑b": {"emoji": "😛", "description": "Tongue sticking out"},
    ":b": {"emoji": "😛", "description": "Tongue sticking out"},
    "d:": {"emoji": "😜", "description": "Playful"},
    "=p": {"emoji": "😛", "description": "Tongue sticking out"},
    ">:P": {"emoji": "😛", "description": "Tongue sticking out"},
    
    ":-/": {"emoji": "🫤", "description": "Skeptical"},
    ":/": {"emoji": "🫤", "description": "Skeptical"},
    ":‑.": {"emoji": "🤔", "description": "Skeptical"},
    ">:\\": {"emoji": "🤔", "description": "Skeptical"},
    ">: /": {"emoji": "🤔", "description": "Skeptical"},
    ":\\": {"emoji": "🤔", "description": "Skeptical"},
    "=/": {"emoji": "🫤", "description": "Skeptical"},
    "=\\": {"emoji": "🫤", "description": "Skeptical"},
    "=L": {"emoji": "😕", "description": "undecided"},
    ":S": {"emoji": "😟", "description": "uneasy"},
    
    ":‑|": {"emoji": "😐", "description": "Straight face"},
    ":|": {"emoji": "😐", "description": "Straight face"},
    
    ":$": {"emoji": "😳", "description": "blushing"},
    "://)": {"emoji": "😳", "description": "blushing"},
    "://3": {"emoji": "😳", "description": "blushing"},
    
    ":‑X": {"emoji": "🤐", "description": "tongue-tied"},
    ":X": {"emoji": "🤐", "description": "tongue-tied"},
    ":‑#": {"emoji": "🤐", "description": "tongue-tied"},
    ":#": {"emoji": "🤐", "description": "tongue-tied"},
    ":‑&": {"emoji": "🤐", "description": "tongue-tied"},
    ":&": {"emoji": "🤐", "description": "tongue-tied"},
    
    "O:‑)": {"emoji": "😇", "description": "Angel"},
    "O:)": {"emoji": "😇", "description": "Angel"},
    "0:‑3": {"emoji": "😇", "description": "Angel"},
    "0:3": {"emoji": "😇", "description": "Angel"},
    "0:‑)": {"emoji": "😇", "description": "Angel"},
    "0:)": {"emoji": "😇", "description": "Angel"},
    "0;^)": {"emoji": "😇", "description": "Angel"},
    
    ">:‑)": {"emoji": "😈", "description": "devilish"},
    ">:)": {"emoji": "😈", "description": "devilish"},
    "}:‑)": {"emoji": "😈", "description": "devilish"},
    "}:)": {"emoji": "😈", "description": "devilish"},
    "3:‑)": {"emoji": "😈", "description": "devilish"},
    "3:)": {"emoji": "😈", "description": "devilish"},
    ">:‑)": {"emoji": "😈", "description": "devilish"},
    ">:)": {"emoji": "😈", "description": "devilish"},
    ">:3": {"emoji": "😈", "description": "devilish"},
    ">:3": {"emoji": "😈", "description": "devilish"},
    
    "|;‑)": {"emoji": "😎", "description": "Cool"},
    "|‑O": {"emoji": "😪", "description": "bored"},
    "B-)": {"emoji": "😎", "description": "Cool"},
    
    ":‑J": {"emoji": "😏", "description": "Tongue-in-cheek"},
    
    "#‑)": {"emoji": "🥴", "description": "Partied all night"},
    "%‑)": {"emoji": "😵", "description": "confused"},
    "%)": {"emoji": "😵", "description": "confused"},
    
    ":‑###..": {"emoji": "🤒", "description": "Being sick"},
    ":###..": {"emoji": "🤒", "description": "Being sick"},
    
    "<:‑|": {"emoji": "😶", "description": "Dumb"},
    "',:-|": {"emoji": "🤨", "description": "Scepticism"},
    "',:-l": {"emoji": "🤨", "description": "Scepticism"},
   
    ":E": {"emoji": "😬", "description": "awkward"},
    
    "8-X": {"emoji": "☠️", "description": "Skull and crossbones"},
    "8=X": {"emoji": "☠️", "description": "Skull and crossbones"},
    "x-3": {"emoji": "☠️", "description": "Skull and crossbones"},
    "x=3": {"emoji": "💀", "description": "Skull and crossbones"},
    "☠️": {"emoji": "💀", "description": "Skull and crossbones"},
    
    "~:>": {"emoji": "🐔", "description": "Chicken"},
    
    "@};-": {"emoji": "🌹", "description": "Rose"},
    "@}->--": {"emoji": "🌹", "description": "Rose"},
    "@}‑;‑'‑‑‑": {"emoji": "🌹", "description": "Rose"},
    "@>‑‑>‑‑": {"emoji": "🌹", "description": "Rose"},
    "8====D": {"emoji": "🍆", "description": "Penis"},
    "8===D": {"emoji": "🍆", "description": "Penis"},
    "8=D": {"emoji": "🍆", "description": "Penis"},
    "3=D": {"emoji": "🍆", "description": "Penis"},
    "8=>": {"emoji": "🍆", "description": "Penis"},
    "8===D~~~": {"emoji": "🍆", "description": "Ejaculation"},
    "*<|:‑)": {"emoji": "🎅", "description": "Santa Claus"},
    "</3": {"emoji": "💔", "description": "Broken heart"},
    "<\\3": {"emoji": "💔", "description": "Broken heart"},
    "<3": {"emoji": "❤️", "description": "Heart"},
    "><>": {"emoji": "🐟", "description": "Fish"},
    "<><": {"emoji": "🐟", "description": "Fish"},
    "<*)))‑{": {"emoji": "🐟", "description": "Fish"},
    "><(((*>": {"emoji": "🐟", "description": "Fish"},
    "\\o/": {"emoji": "🍻", "description": "Cheers"},
    "*\\0/*": {"emoji": "📣", "description": "Cheerleader"},
    "o7": {"emoji": "🫡", "description": "Salute"},
    "v.v": {"emoji": "😔", "description": "Sadness"},
    "._.": {"emoji": "😔", "description": "Sadness"},
    "._.;": {"emoji": "😔", "description": "Sadness"},
    "QQ": {"emoji": "😭", "description": "Crying"},
    "qq": {"emoji": "😭", "description": "Crying"},
    "Qq": {"emoji": "😭", "description": "Crying"},
    "X_X": {"emoji": "😵", "description": "Dead person"},
    "x_x": {"emoji": "😵", "description": "Dead person"},
    "+_+": {"emoji": "😵", "description": "fainted"},
    "X_x": {"emoji": "😵", "description": "Dead person"},
    "x_X": {"emoji": "😵", "description": "Dead person"},
    "<_<": {"emoji": "😏", "description": "Sideways look. Devious or guilty."},
    ">_>": {"emoji": "😏", "description": "Sideways look. Devious or guilty."},
    "<.<": {"emoji": "😏", "description": "Sideways look. Devious or guilty."},
    ">.>": {"emoji": "😏", "description": "Sideways look. Devious or guilty."},
    "O_O": {"emoji": "😳", "description": "Surprise"},
    "o_o": {"emoji": "😳", "description": "Surprise,"},
    "O-O": {"emoji": "😳", "description": "Surprise,"},
    "o‑o": {"emoji": "😳", "description": "Surprise,"},
    "O_o": {"emoji": "😳", "description": "Surprise,"},
    "o_O": {"emoji": "😳", "description": "Surprise,"},
    ">.<": {"emoji": "😣", "description": "Skeptical"},
    ">_<": {"emoji": "😣", "description": "Skeptical"},
    "^5": {"emoji": "🖐️", "description": "High five"},
    "o/\\o": {"emoji": "🖐️", "description": "High five"},
    ">_>^ ^<_<": {"emoji": "🖐️", "description": "High five"},
    "V.v.V": {"emoji": "🦀", "description": "Crab"},
    "V=(° °)=V": {"emoji": "🦞", "description": "Lobster"},
    "(^^^)": {"emoji": "🦈", "description": "Shark"},
    "(::[]::)": {"emoji": "🩹", "description": "Bandage"},
    "(o)(o)": {"emoji": "🍈", "description": "Breasts"},
    "( • )( • )": {"emoji": "🍈", "description": "Breasts"},
    "(. Y .)": {"emoji": "🍈", "description": "Breasts"}
}

html_entity_dict = {
    "&lt;": "<",
    "&gt;": ">",
    "&le;": "≤",
    "&ge;": "≥",
    "&gt;=": ">=",
    "&lt;=": "<="
}

def replace_html_entities(text):
    for entity, symbol in html_entity_dict.items():
        text = text.replace(entity, symbol)
    return text


# Your existing emoji_dict code here

def clean_text(text):
    text = re.sub(r'<[^>]+>', '', text)  # Remove HTML tags
    text = re.sub(r'@\w+', '', text)  # Remove @ mentions
    text = re.sub(r'http[s]?://\S+', '', text)  # Remove URLs
    text = replace_html_entities(text)  # Replace HTML entities
    return text


def emoji_to_unicode(text):
    
    return text

def find_emoticons(text):
    found_emoticons = []
    for emoticon, details in emoji_dict.items():
        if re.search(r'\b' + re.escape(emoticon)+ r'\b', text):
            found_emoticons.append(details['emoji'])
    return ','.join(found_emoticons)

def remove_emojis(text):
    for emoticon in emoji_dict:
        text = text.replace(emoticon, '')  # Remove the emoticon symbols from the text
    return text

def emoji_to_unicode_details(emoji):
    # Convert each Unicode character in the emoji to a hexadecimal code point
    return ' '.join(f'U+{ord(char):04X}' for char in emoji)

def emoji_to_unicode(text):
    for emoticon, details in emoji_dict.items():
        # Use the previously defined function to get Unicode code points
        unicode_representation = emoji_to_unicode_details(details['emoji'])
        # Replace emoticons in the text with their Unicode representation
        text = re.sub(r'\b' + re.escape(emoticon) + r'\b', unicode_representation, text)
    return text

def remove_all_special_characters(text):
    found_emoticons = []
    for emoticon, details in emoji_dict.items():
        if re.search(r'\b' + re.escape(emoticon) + r'\b' , text):
            found_emoticons.append(details['emoji'])  # Remove all non-word characters and non-spaces
            text.replace(emoticon,'')
    return text



# Load data
df = dd.read_csv('/Users/praneeth/Downloads/term_project/tweet_data.csv', usecols=[0, 5], names=['emotion', 'tweet'], header=None, encoding='latin1')

# Apply text cleaning
df['tweet'] = df['tweet'].map_partitions(lambda part: part.apply(clean_text), meta=('tweet', str))

# Apply emoticon find function
df['emoticons'] = df['tweet'].map(find_emoticons, meta=('emoticons', str))

# Remove emojis from tweets
df['tweet_no_emojis'] = df['tweet'].map(remove_emojis, meta=('tweet_no_emojis', str))

# Convert emojis to their Unicode characters
df['tweet_unicode_emojis'] = df['tweet'].map(emoji_to_unicode, meta=('tweet_unicode_emojis', str))

# Remove all special characters
df['tweet_no_special_chars'] = df['tweet'].map(remove_all_special_characters, meta=('tweet_no_special_chars', str))

# Filter rows that contain at least one emoticon
df_with_emojis = df[df['emoticons'].map(lambda x: len(x) > 0, meta=('x', 'bool'))]

# Sample data if necessary
with ProgressBar():
    total_count = df_with_emojis.shape[0].compute()
    sampled_df = df_with_emojis.sample(frac=(10000 / total_count) if total_count > 10000 else 1, random_state=1)

# Save sampled data
output_path = "/Users/praneeth/Downloads/term_project/sampled.csv"
with ProgressBar():
    sampled_df.to_csv(output_path, index=False, single_file=True)
