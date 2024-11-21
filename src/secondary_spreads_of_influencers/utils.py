#!/usr/bin/env python3

import re
import vibrato
import zstandard
import neologdn
import numpy as np
from scipy.special import kl_div
import matplotlib.colors as mcolors
import numpy as np


BRACKET = "\\[|\\(|\\（|\\【|\\{|\\〈|\\［|\\｛|\\＜|\\｜|\\|"
EMOTICON_CHARS = "￣|◕|´|_|ﾟ|・|｀|\\-|\\^|\\ |･|＾|ω|\\`|＿|゜|∀|\\/|Д|　|\\~|д|T|▽|o|ー|\\<|。|°|∇|；|ﾉ|\\>|ε|\\)|\\(|≦|\\;|\\'|▼|⌒|\\*|ノ|─|≧|ゝ|●|□|＜|＼|0|\\.|○|━|＞|\\||O|ｰ|\\+|◎|｡|◇|艸|Ｔ|’|з|v|∩|x|┬|☆|＠|\\,|\\=|ヘ|ｪ|ェ|ｏ|△|／|ё|ロ|へ|０|\"|皿|．|3|つ|Å|、|σ|～|＝|U|\\@|Θ|‘|u|c|┳|〃|ﾛ|ｴ|q|Ｏ|３|∪|ヽ|┏|エ|′|＋|〇|ρ|Ｕ|‐|A|┓|っ|ｖ|∧|曲|Ω|∂|■|､|\\:|ˇ|p|i|ο|⊃|〓|Q|人|口|ι|Ａ|×|）|―|m|V|＊|ﾍ|\\?|э|ｑ|（|，|P|┰|π|δ|ｗ|ｐ|★|I|┯|ｃ|≡|⊂|∋|L|炎|З|ｕ|ｍ|ｉ|⊥|◆|゛|w|益|一|│|о|ж|б|μ|Φ|Δ|→|ゞ|j|\\\\|\\    |θ|ｘ|∈|∞|”|‥|¨|ﾞ|y|e|\\]|8|凵|О|λ|メ|し|Ｌ|†|∵|←|〒|▲|\\[|Y|\\!|┛|с|υ|ν|Σ|Α|う|Ｉ|Ｃ|◯|∠|∨|↑|￥|♀|」|“|〆|ﾊ|n|l|d|b|X|ó|Ő|Å|癶|乂|工|ш|ч|х|н|Ч|Ц|Л|ψ|Ψ|Ο|Λ|Ι|ヮ|ム|ハ|テ|コ|す|ｙ|ｎ|ｌ|ｊ|Ｖ|Ｑ|√|≪|⊇|⊆|＄|″|♂|±|｜|ヾ|？|：|ﾝ|ｮ|f|\\%|ò|å|冫|冖|丱|个|凸|┗|┼|ц|п|Ш|А|φ|τ|η|ζ|β|α|Γ|ン|ワ|ゥ|ぁ|ｚ|ｒ|ｋ|ｄ|ｂ|Ｘ|Ｐ|Ｈ|Ｄ|８|♪|≫|↓|＆|「|［|々|仝|!|ﾒ|ｼ|｣"
RE_EMOTICON = re.compile('('+BRACKET+')(['+EMOTICON_CHARS+']{3,}).*')
SYMBOLS = {"MENTIONSYM", "EMOSYM", "IMGSYM", "URLSYM"}

STOPWORDS = ["日", "中", "・ﾟ", "・", "する"]
_vib_tokenizer = None

CLUSTER_NUMBER_TO_NAME = {
    0: "Others (Popular Topics)",
    1: "Promotions",
    2: "VTuber \& Games for Male",
    3: "Lifelog \& Animal",
    4: "Politics (Left)",
    5: "Games \& Anime Goods for Female",
    6: "Politics (Right)",
    7: "Japanese Male Idol",
    8: "Baseball \& Soccer",
    9: "Japanese Female Idol",
    10: "Streamer \& Internet Singer \& Vocaloid",
    12: "Korean Idol",
    14: "Anime \& Voice Actor",
    17: "NSFW \& Spam",
    18: "YouTuber",
    20: "Collectible Card Game",
    29: "Games",
}


def generate_cmap(colors, cmap_name = 'custom_cmap'):
    values = range(len(colors))
    vmax = np.ceil(np.max(values))
    color_list = []
    for vi, ci in zip(values, colors):
        color_list.append( ( vi/ vmax, ci) )

    return mcolors.LinearSegmentedColormap.from_list(cmap_name, color_list, 256)


CMAP_CMTHERMAL = generate_cmap(['#1c3f75', '#068fb9','#f1e235', '#d64e8b', '#730e22'], 'cmthermal')


def identity(x):
    return x


def symbolize_mention(text):
    return re.sub(r"@[a-zA-Z0-9_]+", " MENTIONSYM ", text)


def symbolize_emoticon(text):
    return RE_EMOTICON.sub(" EMOSYM ", text)


def symbolize_url(text):
    # https://pbs.twimg.com/から始まるURLはIMGSYMに置換
    text = re.sub(r"https://pbs.twimg.com/[\w/:%#\$&\?\(\)~\.=\+\-]+", " IMGSYM ", text)

    # https://video.twimg.com/から始まるURLはVIDEOSYMに置換
    text = re.sub(r"https://video.twimg.com/[\w/:%#\$&\?\(\)~\.=\+\-]+", " VIDEOSYM ", text)

    # https?://から始まるURLはURLSYMに置換
    # HOSTNAMEをSYMNOLにするということは可能？
    # tokenizerで1単語で認識
    return re.sub(r"h?ttps?://?[\w/:%#\$&\?\(\)~\.=\+\-]+", " URLSYM ", text)


def load_tokenizer():
    dctx = zstandard.ZstdDecompressor()
    with open("/data_pwdtms04/niitsuma-t/projects/vibrato-dict-ipa-neologd/dict/mecab-ipadic-neologd.dic.zst", "rb") as fp:
        with dctx.stream_reader(fp) as reader:
            tokenizer = vibrato.Vibrato(reader.read())
    return tokenizer


def is_content_word(pos):
    if pos[0] == "名詞" and pos[1] in {"一般", "固有名詞", "サ変接続", "形容動詞語幹"}:
        return True

    if pos[0] in {"形容詞", "動詞"} and pos[1] == "自立":
        return True
    return False


def clean_text(text, symbolize={"mention", "emoticon", "url"}, remove_nlsp=True):
    text = neologdn.normalize(text, tilde="normalize")
    text = re.sub(r"(.+)(https?)", "\\1 \\2", text)

    if "url" in symbolize:
        text = symbolize_url(text)

    if "emoticon" in symbolize:
        text = symbolize_emoticon(text)

    if "mention" in symbolize:
        text = symbolize_mention(text)

    if remove_nlsp:
        text = text.replace("\u200b", " ")
        text = re.sub(r"(\n|\r\n|\r)+", "\n", text)
        text = re.sub(r"\s+", " ", text)

    return text


class Tokenizer:
    def __init__(self, symbolize={"mention", "emoticon", "url"}):
        self.tokenizer = load_tokenizer()
        self.symbolize = symbolize

    def __call__(self, text):
        text = clean_text(text, self.symbolize)

        ms = []
        for m in self.tokenizer.tokenize(text):
            feature = m.feature().split(",")
            pos = feature[0:3]
            lemma = feature[6]
            if not is_content_word(pos):
                continue
            if lemma is None or lemma == "*":
                lemma = m.surface()
            if lemma.isascii():
                if pos[1] != "固有名詞" and lemma not in SYMBOLS:
                    lemma = lemma.lower()
            ms.append(lemma)
        return ms


def tokenize_text(text):
    # クロージャで実装すべき？
    global _vib_tokenizer
    if _vib_tokenizer is None:
        _vib_tokenizer = load_tokenizer()

    text = clean_text(text)

    ms = []
    for m in _vib_tokenizer.tokenize(text):
        feature = m.feature().split(",")
        pos = feature[0:3]
        lemma = feature[6]
        if not is_content_word(pos):
            continue
        if lemma is None or lemma == "*":
            lemma = m.surface()
        if lemma.isascii():
            if pos[1] != "固有名詞" and lemma not in SYMBOLS:
                lemma = lemma.lower()
        ms.append(lemma)
    return ms


def mean_fasttext(words_list, fasttext, dim=50):
    embeds = []
    for words in words_list:
        words = [word for word in words if word in fasttext.wv]
        if len(words) == 0:
            embeds.append(np.zeros(dim))
        else:
            embeds.append(np.mean(fasttext.wv[words], axis=0))
    return np.array(embeds)


def diff_of_bio_cluster(df_bio_cluster, target_user_ids, method="kld", return_dist=False):
    dist_all = df_bio_cluster["cluster"].value_counts(normalize=True)

    dist_target = df_bio_cluster.filter(
        df_bio_cluster["user_id"].is_in(target_user_ids)
    )["cluster"].value_counts(normalize=True)

    clusters = dist_all[["cluster"]]
    dist_target = dist_target.join(
        clusters, on="cluster", how="outer").fill_null(0)

    dist_all = dist_all.sort("cluster")["proportion"]
    dist_target = dist_target.sort("cluster")["proportion"]

    if method == "kld":
        kld = kl_div(dist_target, dist_all)
        if return_dist:
            return kld.sum(), (kld, dist_all, dist_target)
        return kld.sum()
    elif method == "bhattacharyya":
        bhat = bhattacharyya_distance(dist_target, dist_all)
        return bhat
    else:
        raise ValueError(f"Unknown method: {method}")


def bhattacharyya_distance(p, q):
    p = np.array(p)
    q = np.array(q)
    diff_sum = np.sum(np.sqrt(p * q))
    return -np.log(diff_sum)


def diff_of_bio_cluster_between_two(df_bio_cluster, user_ids1, user_ids2, method="kld"):
    dist_all = df_bio_cluster["cluster"].value_counts()

    dist_target1 = df_bio_cluster.filter(
        df_bio_cluster["user_id"].is_in(user_ids1)
    )["cluster"].value_counts(normalize=True)

    dist_target2 = df_bio_cluster.filter(
        df_bio_cluster["user_id"].is_in(user_ids2)
    )["cluster"].value_counts(normalize=True)

    clusters = dist_all[["cluster"]]
    dist_target1 = dist_target1.join(
        clusters, on="cluster", how="outer").fill_null(0)
    dist_target1 = dist_target1.sort("cluster")["proportion"]

    dist_target2 = dist_target2.join(
        clusters, on="cluster", how="outer").fill_null(0)
    dist_target2 = dist_target2.sort("cluster")["proportion"]

    if method == "kld":
        kld = kl_div(dist_target1, dist_target2)
        return kld.sum()
    elif method == "jsd":
        jsd = (kl_div(dist_target1, (dist_target1 + dist_target2) / 2) +
               kl_div(dist_target2, (dist_target1 + dist_target2) / 2)) / 2
        return jsd.sum()
    elif method == "bhattacharyya":
        return bhattacharyya_distance(dist_target1, dist_target2)
    else:
        raise ValueError(f"Unknown method: {method}")
