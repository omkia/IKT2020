from elasticsearch import Elasticsearch
import csv
from nltk.stem.wordnet import WordNetLemmatizer
import stanza
import nltk

stanza.download('en')
nltk.download('stopwords')
nltk.download('punkt')
nlp = stanza.Pipeline(lang='en', processors='tokenize,ner')

elastic_client = Elasticsearch(timeout=40, max_retries=10, retry_on_timeout=True)
# only wait for 1 second, regardless of the client's default
elastic_client.cluster.health(wait_for_status='yellow', request_timeout=40)


def entities(doc):
    # sample input:
    # doc = nlp("What is the PA average salary vs an RN?")
    # print(*[f'entity: {ent.text}\ttype: {ent.type}' for ent in doc.ents], sep='\n')
    entities = []
    for ent in doc.ents:
        entities.append(ent.type)
    return entities


def lemma(sentence):
    # sentence = "He was running and eating at same time. He has bad habit of swimming after playing long hours in
    # the Sun."
    lemmatizer = WordNetLemmatizer()
    return lemmatizer.lemmatize(sentence.casefold())


def CheckStringContainAnyItem(test_string, test_list):
    # checking if string contains list element
    # initializing string
    # test_string = "There are 2 apples for 4 persons"
    # initializing test list
    # test_list = ['apples', 'oranges']
    res = [ele for ele in test_list if (lemma(ele) in lemma(test_string))]
    # print result
    return bool(res)


def searchQueryandRerank(firstturn, prevturn, currentturn, summary, turn_num, turnname, index):
    # sample input
    # query = 'hi'
    # turn_num= "1_1"
    query = "(" + firstturn + ")^2 (" + prevturn + ")^1 (" + currentturn + ")^3 (" + summary + ")^4"
    print(query)
    search_param = {
        "query": {
            "query_string": {
                "query": query,
                "default_field": "doc.content"
            }
        }
    }
    # get another response from the cluster
    response = elastic_client.search(index=index, body=search_param, request_timeout=40, size="1000")
    all_hits = response['hits']['hits']
    ranking = ""
    rank = 0
    scores = []
    WhyWords = ["reason", "because", "causing", "so", "why", "since", "due to", "result of", "therefore"]
    WhereWords = ["source", "originate", "originating", "origin", "part of", "world", "native", "country", "countries",
                  "place", "Ancient", "north", "south", "east", "west", "gulf", "ocean", "site", "park", "area",
                  "miles", "downtown", "island", "antarctic", "harbour"]
    WhenWords = ["spring", "summer", "autumn", "winter", "saturday", "sunday", "monday", "tuesday", "wednesday",
                 "thursday", "friday", "year", "time", "age", "early", "BC", "BCE", "until", "millenium", "era",
                 "century", "when", "season", "later"]
    TellWords = ["like", "related", "most", "described", "define", "detail", "known", "fact", "review", "summary",
                 "example", "illustrate", "how", "mean", "contain", "consist", "constituent", "structure", "character",
                 "almost", "common", "class", "family"]
    i = 0
    for num, doc in enumerate(all_hits):
        i += 1
        if (i < 11):
            print(str(i) + "                " + doc['_source']['doc']['content'])
        scores.append(str(doc['_score']))
    for num, doc in enumerate(all_hits):
        if "Where" in currentturn:
            if ("GPE" in entities(nlp(doc['_source']['doc']['content'])) or CheckStringContainAnyItem(
                    doc['_source']['doc']['content'], WhereWords)):
                print("Where   " + doc['_source']['doc']['content'])
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")
        elif "Tell" in currentturn:
            if CheckStringContainAnyItem(doc['_source']['doc']['content'], TellWords):
                print("Tell   " + doc['_source']['doc']['content'])
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")
        elif (
                "How many" in currentturn or "How much" in currentturn or "How long" in currentturn or "cost" in currentturn or "salary" in currentturn):
            if ("CARDINAL" in entities(nlp(doc['_source']['doc']['content'])) or "$" in doc['_source']['doc'][
                'content']):
                print("many   " + doc['_source']['doc']['content'])
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")
        elif "When" in currentturn or "era" in currentturn:
            if (("CARDINAL" in entities(nlp(doc['_source']['doc']['content']))) or (
                    "BC" in doc['_source']['doc']['content'] or "BCE" in doc['_source']['doc']['content']
                    or "years" in doc['_source']['doc']['content'])):
                print("when   " + doc['_source']['doc']['content'])
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")
        elif "Why" in currentturn:
            if ("because" in doc['_source']['doc']['content'] or "causing" in doc['_source']['doc']['content']
                    or "Because" in doc['_source']['doc']['content'] or "result of" in doc['_source']['doc'][
                        'content']):
                print("why   " + doc['_source']['doc']['content'])
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")
    for num, doc in enumerate(all_hits):
        if "Where" in currentturn:
            if not ("GPE" in entities(nlp(doc['_source']['doc']['content'])) or CheckStringContainAnyItem(
                    doc['_source']['doc']['content'], WhereWords)):
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")
        elif "Tell" in currentturn:
            if not (CheckStringContainAnyItem(doc['_source']['doc']['content'], TellWords)):
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")

        elif (
                "How many" in currentturn or "How much" in currentturn or "How long" in currentturn or "cost" in currentturn or "salary" in currentturn):
            if not ("CARDINAL" in entities(nlp(doc['_source']['doc']['content'])) or "$" in doc['_source']['doc'][
                'content']):
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")
        elif "When" in currentturn or "era" in currentturn:
            if (not (("CARDINAL" in entities(nlp(doc['_source']['doc']['content']))) or (
                    "BC" in doc['_source']['doc']['content'] or "BCE" in doc['_source']['doc']['content']
                    or "years" in doc['_source']['doc']['content']))):
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")
        elif "Why" in currentturn:
            if (not ("because" in doc['_source']['doc']['content'] or "causing" in doc['_source']['doc']['content']
                     or "Because" in doc['_source']['doc']['content'] or "result of" in doc['_source']['doc'][
                         'content'])):
                rank = rank + 1
                ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                    rank - 1] + " " + turnname + "\n")
        else:
            rank = rank + 1
            ranking = ranking + (str(turn_num) + " 0 " + doc["_id"] + " " + str(rank) + " " + scores[
                rank - 1] + " " + turnname + "\n")
    return ranking


def filereader(turnname, index):
    with open("F:\\newest.csv", encoding='utf-8') as tsvfile:
        tsvreader = csv.reader(tsvfile, delimiter=",")
        StopWords = [" do ", " my ", " doing ", " Describe ", "Tell me about"]
        ranks = ""
        for line in tsvreader:
            if line[0] == "sep=":
                continue
            q1 = line[1]
            for stop in StopWords:
                q1 = q1.replace(stop, " ")
            q2 = line[2]
            for stop in StopWords:
                q2 = q2.replace(stop, " ")
            q3 = line[3]
            for stop in StopWords:
                q3 = q3.replace(stop, " ")
            print(q1 + q2 + q3)
            ranks = ranks + searchQueryandRerank(q1, q2, q3, line[4], line[0], turnname, index)
        resultFile = open(turnname + ".txt", "w")  # write mode
        resultFile.write(ranks)  # resultFile
        resultFile.close()


filereader("RuleBased", "lmd")
