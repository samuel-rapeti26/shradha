# File for functions

# import libraries
from azure.cosmos import CosmosClient
from configparser_crypt import ConfigParserCrypt
import hashlib
import uuid
import pandas as pd
import pyreadr
import os
import numpy as np
from collections import Counter
import regex as re
import spacy_udpipe
from datetime import datetime
from hunspell import Hunspell
from itertools import repeat, chain, accumulate
import sys

# decrypt the config file
file = './db_config.ini'
conf_file = ConfigParserCrypt()
# Set AES key
with open('./filekey.key', 'rb') as filekey:
    aes_key = filekey.read()
conf_file.aes_key = aes_key
# Read encrypted config file
conf_file.read_encrypted(file)

# connect to DB
client = CosmosClient(url=conf_file['db_access']["uri"], credential=conf_file['db_access']["key"])
# user table
db_name = conf_file['db_access']["database_name"]
database = client.get_database_client(db_name)

# function to hash the user password
def hashpass(password, salt):
    key = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 1000000, dklen=None)
    return key

# function to validate user in DB
def UserLogin(data):
    # set connection
    con_name = conf_file['db_access']["container_user"]
    container = database.get_container_client(con_name)
    
    # select entries matching to user id
    user = []
    for item in container.query_items(query=f"SELECT * FROM {con_name} t WHERE t.User=@user", parameters = [{"name": "@user", "value": data["User"]}], enable_cross_partition_query = True):
        user.append(item)
    
    # compare user info with DB
    if len(user) != 0:
        # select first user
        user = user[0]
        salt1 = bytes(user["PasswordSalt"], 'latin1')
        hashpassword = hashpass(data["Password"], salt1)
        # if the above hashed password is matching to the one that user send then we will create JWT
        if hashpassword == bytes(user["Password"], 'latin1'):
            if user["Admin"] == "Y":
                return True, "Admin"
            else:
                return True, "Non-Admin"
        else:
            return False, "Incorrect Password."
    else:
        return False, "User ID not found."
        

# function to add accepted words by admin
def add_word(data):
    
    # set connection
    con_name = conf_file['db_access']["container_addwords"]
    container = database.get_container_client(con_name)
      
    # add the data to DB table
    # data is dict with values as list and keys are word, time, user
    try:
        for i in range(len(data["word"])):
            newdata = {"id": str(uuid.uuid4()), "Words": data["word"][i], "Time": data["time"][i], "User": data["user"][i]}
            new = container.create_item(newdata)
        return True
    except:
        return False    


# function to remove rejected words       
def reject_word(data):
    
    # set connection
    con_name = conf_file['db_access']["container_temp"]
    container = database.get_container_client(con_name)
    
    # delete the records obtained by query_items
    try:
        for i in range(len(data["word"])):
            for item in container.query_items(query=f"SELECT t.id FROM {con_name} t WHERE t.Words=@word AND t.User=@user AND t.Time=@time",
                                              parameters = [{"name": "@word", "value": data["word"][i]}, 
                                                            {"name": "@user", "value": data["user"][i]}, 
                                                            {"name": "@time", "value": data["time"][i]}],
                                              enable_cross_partition_query=True):
                container.delete_item(item=item['id'], partition_key={})
        return True
    
    except Exception as e:
        #print(e)
        return False
 

# function to send request for new additions
def update_dictionary(data):
    
    # set connection 
    con_name = conf_file['db_access']["container_temp"]
    container = database.get_container_client(con_name)
    
    # add the data to DB temp table
    # data is dict with values as list and keys are word, time, user
    try:
        for i in range(len(data["word"])):
            newdata = {"id": str(uuid.uuid4()), "Words": data["word"][i], "Time": data["time"][i], "User": data["user"][i]}
            new = container.create_item(newdata)
        return True
    except:
        return False    


# function to view temp table       
def get_temp():
    
    # set connection
    con_name = conf_file['db_access']["container_temp"]
    container = database.get_container_client(con_name)
    
    # fetch data from DB table
    temp_dict = {}
    i = 0
    for item in container.query_items(query=f'SELECT t.Time, t.User, t.Words FROM {con_name} t', enable_cross_partition_query = True):
        temp_dict[i] = item
        i = i + 1
    return temp_dict
    
    
# function to display dictionaries
def get_dictionary():
    
    # set connection
    con_name = conf_file['db_access']["container_addwords"]
    container = database.get_container_client(con_name)
    
    # fetch additional words table
    add_words_list = {}
    i = 0
    for item in container.query_items(query=f'SELECT t.Time, t.User, t.Words FROM {con_name} t', enable_cross_partition_query = True):
        add_words_list[i] = item
        i = i + 1
    
    # if add_words_list != []:
        # d1 = add_words_list[0]
        # for i in range(len(add_words_list)-1):
            # d2 = add_words_list[i+1]
            # d3 = {**d1, **d2}
            # for key, value in d3.items():
                # if key in d1 and key in d2:
                    # d3[key] = [d1[key], value]
            # d1 = d3
    # else:
        # d1 = []
    
    curr_dir = os.getcwd()
    os.chdir("./Input")
    
    # for accepted abbrev table
    with open('./Abbrev_Exceptions_DrugName.txt') as f:
        accepted_abbrev = f.readlines()
    # remove the empty lines
    accepted_abbrev = [i.replace('\n','') for i in accepted_abbrev]
    
    # for product list 
    vaccineList = pd.read_excel("./Vaccine List.xlsx", sheet_name='Sheet1')
    prodlist = [i for i in vaccineList[vaccineList['Type'] == "Prod Name"]['Description'].astype(str).values]
    
    # for unit codes (R3)
    codes = pyreadr.read_r('./Unit_code_R3.RDS')
    unit_dict = codes[None].to_dict("list")
    
    os.chdir(curr_dir)
    
    return {"additional_words": add_words_list,
    "accepted_abbrev": accepted_abbrev, 
    "product_list": prodlist, 
    "unit_codes": unit_dict}    


# main function to get error summary table
# function to connect with db
def conn():
    
    #set connection
    con_name = conf_file['db_access']["container_addwords"]
    container = database.get_container_client(con_name)
    
    # fetch all records for user defined words
    add_words_dict = {}
    i = 0
    for item in container.query_items(query=f'SELECT * FROM {con_name}', enable_cross_partition_query =True):
        add_words_dict[i] = item
        i = i + 1
    return add_words_dict


def prepareData(inputData):
    try:
        if inputData==None or len(inputData)==0:
            return pd.DataFrame()
        
        paraGraphNum=wordDetected=suggestedWord=[]
        errorCode=category=startPos=endPos=operation=frontendAction=[]
    
        ### 1 and 8. Known / Unknown gender ###
        femaleGenderCheck=[]
        maleGenderCheck=[]
        for j in range(len(inputData)):
            m=re.findall(r'\b(female)\b([\W]+\b(patient|subject)\b){0,1}',inputData[j],flags=re.IGNORECASE)
            if len(m)==0:
                femaleGenderCheck.append([])
            femaleGenderCheck.append([(m[i][0] + m[i][1]) for i in range(len(m)) if len(m)!=0])
            n=re.findall(r'\b(male)\b([\W]+\b(patient|subject)\b){0,1}',inputData[j],flags=re.IGNORECASE)
            if len(n)==0:
                maleGenderCheck.append([])
            maleGenderCheck.append([(n[i][0] + n[i][1]) for i in range(len(n)) if len(n)!=0])
        male=False
        female=False
        if len(maleGenderCheck)>0 and len(femaleGenderCheck)>0:
            for j in range(len(inputData)):
                for k in range(len(maleGenderCheck[j])):
                    if re.search(r'\b(patient|subject)\b',maleGenderCheck[j][k],flags=re.IGNORECASE):
                        femaleGenderCheck=[]
                        male=True
                        break
                    else:
                        continue
                if male:
                    break
                for l in range(len(femaleGenderCheck[j])):
                    #if re.search(r'\b(patient|subject)\b',femaleGenderCheck[j][k],flags=re.IGNORECASE):
                    if re.search(r'\b(patient|subject)\b',femaleGenderCheck[j][l],flags=re.IGNORECASE):
                        maleGenderCheck=[]
                        female=True
                        break
                    else:
                        continue
                if female:
                    break
        msum=0
        fsum=0
        for j in range(len(inputData)):
            if len(maleGenderCheck)==0:
                break
            if len(maleGenderCheck[j])==0:
                mflag=1
            else:
                mflag=0
            if mflag:
                maleGenderCheck[j]=[]
            msum+=mflag
        if msum==len(inputData):
            maleGenderCheck=[]

        for j in range(len(inputData)):
            if len(femaleGenderCheck)==0:
                break
            if len(femaleGenderCheck[j])==0:
                fflag=1
            else:
                fflag=0
            if fflag:
                femaleGenderCheck[j]=[]
            fsum+=fflag
        if fsum==len(inputData):
            femaleGenderCheck=[]

        actual=['He','he','His','his','him','She','she','Her','her','himself','herself','hers']
        replace=['She','she','Her','her','her','He','he', 'His', 'him/ his','herself','himself','his']
        genChange=pd.DataFrame(data={'actual':actual,'replace':replace})
        genChange.head()
        if len(maleGenderCheck)!=0:
            for j in range(len(inputData)):
                replacedWord=re.finditer(r'\b(she|her)\b|(?<=[ .,])(hers|herself)(?=[ .,])',inputData[j],flags=re.IGNORECASE)
                for k in replacedWord:
                    if re.search(r'^[A-Z]+$',k.group(0)):
                        next
                    paraGraphNum=paraGraphNum + [j+1]
                    wordDetected=wordDetected + [k.group(0)]
                    tempWord1=[k.group(0).title() if re.search(r'^[A-Z]',k.group(0)) else k.group(0).lower()]
                    suggest=genChange[genChange['actual']==tempWord1[0]].iat[0,1]
                    suggestedWord=suggestedWord + [suggest]
                    errorCode=errorCode + ['Incorrect gender (expected: Male)']
                    category=category + ['Optional']
                    startPos=startPos + [k.start()]
                    endPos=endPos + [k.end()]
                    operation=operation + ['male']
                    frontendAction=frontendAction + ['Replace']
        elif len(femaleGenderCheck)!=0:
            for j in range(len(inputData)):
                replacedWord=re.finditer(r'\b(he|his)\b|(?<=[ .,])(him|himself)(?=[ .,])',inputData[j],flags=re.IGNORECASE)
                for k in replacedWord:
                    if re.search(r'^[A-Z]+$',k.group(0)):
                        next
                    paraGraphNum=paraGraphNum + [j+1]
                    wordDetected=wordDetected + [k.group(0)]
                    tempWord1=[k.group(0).title() if re.search(r'^[A-Z]',k.group(0)) else k.group(0).lower()]
                    suggest=genChange[genChange['actual']==tempWord1[0]].iat[0,1]
                    suggestedWord=suggestedWord + [suggest]
                    errorCode=errorCode + ['Incorrect gender (expected: Female)']
                    category=category + ['Optional']
                    startPos=startPos + [k.start()]
                    endPos=endPos + [k.end()]
                    operation=operation + ['female']
                    frontendAction=frontendAction + ['Replace']
        else:
            for j in range(len(inputData)):
                unk_pat=r'\b(he([ /]*she)*|she([ /]*he)*)\b|(?<=[ .,])(his([ /]*her)*|him([ /]*her)*|her([ /]*(his|him))*|himself([ /]*herself)*|hers|herself([ /]*himself)*)(?=[ .,])'
                replacedWord=re.finditer(unk_pat,inputData[j],flags=re.IGNORECASE)
                for k in replacedWord:
                    if re.search(r'^[A-Z]+$',k.group(0)):
                        next
                    paraGraphNum=paraGraphNum + [j+1]
                    wordDetected=wordDetected + [k.group(0)]
                    suggest=['The patient' if re.search(r'^[A-Z]',k.group(0)) else 'the patient']
                    suggestedWord=suggestedWord + [suggest]
                    errorCode=errorCode + ['Incorrect gender (expected: Unknown)']
                    category=category + ['Optional']
                    startPos=startPos + [k.start()]
                    endPos=endPos + [k.end()]
                    operation=operation + ['unknown']
                    frontendAction=frontendAction + ['Replace']

        
        curr_dir = os.getcwd()
        os.chdir("./Input")
    
        ### 2. Wrong Pronouns ###
        fileObject = open("./Wrong_pronouns.txt", "rt")
        data = fileObject.read()
        for j in range(len(inputData)):
            replacedWord=re.finditer(data,inputData[j],flags=re.IGNORECASE)
            for k in replacedWord:
                paraGraphNum=paraGraphNum + [j+1]
                wordDetected=wordDetected + [k.group(0)]
                suggest=''
                suggestedWord=suggestedWord + [suggest]
                errorCode=errorCode + ['Wrong pronoun used']
                category=category + ['Optional']
                startPos=startPos + [k.start()]
                endPos=endPos + [k.end()]
                operation=operation + ['pronoun']
                frontendAction=frontendAction + ['No action']


        ### 3. Tense ###
        #try:
            # list_1 = os.listdir(path=spacy_udpipe.__path__[0] + '\\models')
        # except FileNotFoundError:
            # list_1 = ""
        # model_path=sorted(list(filter(lambda x: True if re.search('\\.udpipe$',x) else False,list_1)),key=str.lower,reverse=True)
        # if len(model_path)==0:
            #spacy_udpipe.download("en")
        udmodel_english=spacy_udpipe.load("en")    
        input_ann=pd.DataFrame(columns=['doc_id','sentence_id','token_id','token','lemma','upos','xpos','feats','dep_rel'])
        for t in range(len(inputData)):
            doc = udmodel_english(inputData[t])
            for token in doc:
                input_ann.loc[len(input_ann)]=['doc'+ str(t+1),t+1,len(input_ann)+1,token.text, token.lemma_, token.pos_,token.tag_,token.morph,token.dep_]
        input_ann['start']=''
        input_ann['end']=''
        for t in range(len(inputData)):
            para_rows=input_ann[input_ann['doc_id'].isin(['doc' + str(t+1)])].index
            startFrom=0
            for x in para_rows:
                if input_ann.loc[x,'upos'] not in ['PUNCT','NUM']:
                    if re.search(re.escape(input_ann.loc[x,'token']),inputData[t][startFrom:]):
                        pos=re.search(re.escape(input_ann.loc[x,'token']),inputData[t][startFrom:]).span()
                        pos=tuple(map(lambda x:x+startFrom,pos))
                        startFrom=pos[1]
                        input_ann.loc[x,'start']=pos[0]
                        input_ann.loc[x,'end']=pos[1]

        actual=["am", "'m", "is", "'s", "are", "'re", "has", "have", "'ve", "will", "shall", "may", "can", "ca", "do", "does"]
        replaceBy=["was", " was", "was", " was", "were", " were", "had", "had", " had", "would", "should", "might", "could", "could", "did", "did"]
        aux_present=pd.DataFrame(data={'actual':actual,'replaceBy':replaceBy})
        actual=["was", "were", "had", "would", "should", "might", "could"]
        replaceBy=["is", "are", "have/has", "will", "shall", "may", "can"]
        aux_past=pd.DataFrame(data={'actual':actual,'replaceBy':replaceBy})

        verb_data=pd.read_excel("./common-verbs-english.xlsx")
        verb_data['Present'].fillna(value='',inplace=True)
        verb_data['Present']=verb_data.apply(lambda x: x['Present'].strip().lower(),axis=1)
        verb_data['ThirdSingular'].fillna(value='',inplace=True)
        verb_data['ThirdSingular']=verb_data.apply(lambda x: x['ThirdSingular'].strip().lower(),axis=1)
        verb_data['Past'].fillna(value='',inplace=True)
        verb_data['Past']=verb_data.apply(lambda x: x['Past'].strip().lower(),axis=1)
        verb_data['PastParticiple'].fillna(value='',inplace=True)
        verb_data['PastParticiple']=verb_data.apply(lambda x: x['PastParticiple'].strip().lower(),axis=1)
        
        wrong_verbs=pd.read_csv('./wrong_verbs.txt',header=None,names=['wrong_verbs'])
        wrong_verbs=wrong_verbs.loc[wrong_verbs['wrong_verbs'].apply(lambda x: x.lower()).argsort()].reset_index(drop=True)
        para_ids=input_ann['doc_id'].unique().tolist()

        for para_id in para_ids:
            input_para=input_ann[input_ann['doc_id']==para_id]
            sent_ids=input_para['sentence_id'].unique().tolist()
            for sent_id in sent_ids:
                input_sent=input_para[input_para['sentence_id']==sent_id]
                word_indicator=pd.Series([True if re.search('[A-Za-z]',k) else False for k in input_sent['token'].tolist()]).any()
                firstWordIndex=[pd.Series([True if re.search('[A-Za-z]',k) else False for k in input_sent['token'].tolist()]).argmax()]
                if len(firstWordIndex)>0 and word_indicator:
                    input_sent.drop(index=input_sent.index[firstWordIndex[0]],inplace=True)
                verbDetected=pd.DataFrame()
                checkMainVerb=True
                aux_condn=zip((input_sent['upos']=='AUX').tolist(),[not re.search('VerbForm=Inf',str(input_sent['feats'][i])) for i in input_sent.index])
                aux_verbs=input_sent[[i[0] and i[1] for i in aux_condn]]
                input_sent2=input_sent.query("upos=='VERB' and xpos in ['VB','VBP','VBZ']")
                main_verbs=input_sent2[[not re.search('VerbForm=Inf',str(input_sent2['feats'][i])) for i in input_sent2.index]]

                if len(aux_verbs)>0:
                    aux_pres_rows=aux_verbs['token'].str.lower().isin(aux_present['actual'])
                    if aux_pres_rows.any():
                        verb=aux_verbs.loc[aux_pres_rows, "token"]
                        verb.index=pd.RangeIndex(stop=len(verb))
                        verb_st=aux_verbs['start'][aux_pres_rows]
                        verb_st.index=pd.RangeIndex(stop=len(verb_st))
                        verb_ed=aux_verbs['end'][aux_pres_rows]
                        verb_ed.index=pd.RangeIndex(stop=len(verb_ed))
                        excludeRows=[]
                        suggest=start=end=[]
                        for i in range(len(verb)):
                            suggest=suggest + aux_present[aux_present['actual']==verb.str.lower()[i]]['replaceBy'].tolist()
                            st=verb_st[i]
                            ed=verb_ed[i]
                            start=start + [st]
                            end=end + [ed]
                            par=[int(i) for i in re.findall('\\d+',para_id)][0]
                            if not re.search("'t|n't|'ve",inputData[par-1][ed:ed+2].lower()) and re.search("\\w",inputData[par-1][ed:ed+2]):
                                suggest[i]=suggest[i] + " "
                            if verb.str.lower()[i]=='may':
                                date_1="\\b(may[ -/][0-9]{1,2}[ ]{0,2}[,][ ]{0,2}\\d{2,4})\\b"
                                date_2="\\b(\\d{1,2}[ -/]*may[ -/]*\\d{2,4})\\b|\\b(may[ -/]*\\d{2,4})\\b"
                                date_3="\\b(\\d{2,4}[ -/]*may[ -/]*\\d{1,2})\\b|\\b(\\d{2,4}[ -/]*may)\\b"
                                may_pat=date_1 + "|" + date_2 + "|" + date_3
                                checker=inputData[par-1][(st-8):(ed+8)]
                                if re.search(may_pat,checker):
                                    excludeRows=excludeRows + [i]
                        if len(excludeRows)>0:
                                verb=verb.drop(verb.index[i])
                                suggest.pop(i)
                                start.pop(i)
                                end.pop(i)
                        verbDetected=pd.DataFrame(data={'verb':verb,'suggest':suggest,'start':start,'end':end}) 
                    if (len(aux_pres_rows) < len(aux_verbs)) or len(verbDetected) > 0:
                        checkMainVerb=False
                if checkMainVerb and len(main_verbs)>0:
                    main_verbs.index=pd.RangeIndex(stop=len(main_verbs))
                    main_verbs=main_verbs[~main_verbs['token'].str.lower().isin(wrong_verbs['wrong_verbs'])]
                    verb=main_verbs['token']
                    verb.index=pd.RangeIndex(stop=len(verb))
                    suggest=start=end=[]
                    if len(verb)>0:
                        for i in range(len(verb)):
                            verb_data_row=verb_data[verb_data['Present']==verb.str.lower()[i]]
                            if len(verb_data_row)==0:
                                verb_data_row=verb_data[verb_data['ThirdSingular']==verb.str.lower()[i]]
                            suggest=suggest + [verb[i] if len(verb_data_row)==0 else verb_data['Past'][verb_data_row.index[0]]]
                            start=start + [int(main_verbs['start'][i])]
                            end=end + [int(main_verbs['end'][i])]
                        verbDetected=pd.DataFrame(data={'verb':verb,'suggest':suggest,'start':start,'end':end})
                if len(verbDetected)>0:
                    for i in range(len(verbDetected)):
                        if re.search('follow',verbDetected['verb'][i],flags=re.IGNORECASE):
                            nextWords= inputData[int(re.findall('\\d+',para_id)[0])-1][verbDetected['end'][i]+1:verbDetected['end'][i]+20]
                            nextWords=re.findall('\\w+',nextWords) #list
                            if len(nextWords) > 0:
                                nextWord = nextWords[0] #str
                            if not nextWords==[] and re.search('up|Up|uP',nextWord):
                                continue
                        paraGraphNum=paraGraphNum + [int(re.findall('\\d+',para_id)[0])]
                        wordDetected=wordDetected + [verbDetected['verb'][i]]
                        suggestedWord=suggestedWord + [verbDetected['suggest'][i]]
                        errorCode=errorCode + ['Incorrect Tense used']
                        category=category + ['Optional']
                        startPos=startPos + [verbDetected['start'][i]]
                        endPos=endPos + [verbDetected['end'][i]]
                        operation=operation + ['tense']
                        frontendAction=frontendAction + ['Replace']

        
        ### 4. Identify Proper Nouns ###
        input_ann_NNP=input_ann.loc[input_ann['xpos']=='NNP',['doc_id','token','start','end']]
        input_ann_NNP['doc_id']=[int(re.findall("\\d+",i)[0]) for i in input_ann_NNP['doc_id'] if re.search("\\d+",i)]
        def func(d):
            rows=input_ann.loc[(input_ann['xpos']=='NNP') & (input_ann['doc_id']==d),'token'].tolist()
            return rows
        properNouns=list(map(func,['doc'+ str(i+1) for i in range(len(inputData))]))
        exp = re.compile("literature|article", flags=re.IGNORECASE)
        literature_para = [True if exp.search(inputData[j]) else False for j in range(len(inputData))]
        f_med = pyreadr.read_r('./hunspell-med (key-val).RDS')
        hun_med = list(f_med[None]["val"])
        f_add = pyreadr.read_r('./addWords_spellcheck.rds')
        addWords_spellcheck = hun_med + list(f_add[None][None])
        add_words_sugg_all=pyreadr.read_r('./add_words_suggest_all (key-val).RDS')
        addWords_suggest=list(add_words_sugg_all[None]['val'])

        extra_words_dict = conn()
        if extra_words_dict == {}:
            add_words_sugg_all = add_words_sugg_all[None]
        else:
            newWords = list(pd.DataFrame(extra_words_dict).transpose().loc[:, "Words"])
            addWords_spellcheck = addWords_spellcheck + newWords
            addWords_suggest = addWords_suggest + newWords
            l_newWords=[i.lower() for i in newWords]
            add_words_sugg_all=pd.concat([add_words_sugg_all[None],pd.DataFrame(data={'key':l_newWords,'val':newWords})],axis=0)

        
        unitSpellSuggestion = pyreadr.read_r('./unit_spell_suggestion.RDS')
        additionalSpellSuggestion = pyreadr.read_r('./additional_spell_suggestion.RDS')
        unitSpellSuggestion=unitSpellSuggestion[None].transpose().reset_index()
        unitSpellSuggestion.rename(columns = {'index':'key',0:'val'}, inplace = True)
        additionalSpellSuggestion=additionalSpellSuggestion[None].transpose().reset_index()
        additionalSpellSuggestion.rename(columns = {'index':'key',0:'val'}, inplace = True)
        
        h = Hunspell('en_US')
        for i in addWords_suggest:
            h.add(i)
        updatedDict_US_sugg=h
        replacedWordIndex=list(range(len(inputData)))
        for k in range(len(inputData)):
            m=re.findall(r'\b[A-Z]+\b',inputData[k],flags=re.IGNORECASE)
            p = [j for j in m if not updatedDict_US_sugg.spell(j)]
            replacedWordIndex[k]=p
        h_MeD=Hunspell()
        h_MeD.add_dic('./en_Med.dic')
        
        if len(replacedWordIndex)>0:
            for i in range(len(replacedWordIndex)):
                prevErrorEnd=0
                if len(replacedWordIndex[i])!=0:
                    for j in range(len(replacedWordIndex[i])):
                        tempWord=replacedWordIndex[i][j]
                        if not re.match("^[A-Z]+$",tempWord) and not( re.match('^(?=.*[a-zA-Z])(?=.*[0-9])[A-Za-z0-9]+$',tempWord) 
                            and re.search("[A-Za-z]",tempWord) and re.search("[0-9]",tempWord)):
                            h_GB=Hunspell('en_GB')
                            if not h_GB.spell(tempWord):
                                tempWord=tempWord
                            else:
                                tempWord=''
                            if tempWord in addWords_spellcheck:
                                tempWord=''
                            if len(tempWord)>0 and re.match(r".*('s)$",tempWord):
                                tempW=re.sub("('s)$",'',tempWord)
                                if not h_MeD.spell(tempW):
                                    next
                                if not h_GB.spell(tempW):
                                    next
                                if not h.spell(tempW):
                                    next
                            if len(tempWord)>0:
                                ptn=r"\b(" + tempWord + r")\b"
                                wordIndex=re.finditer(ptn,inputData[i])
                                wordlen=re.findall(ptn,inputData[i])
                                if len(wordlen)==0:
                                    next
                                for k in wordIndex:
                                    flag=True
                                    if k.start() in startPos:
                                        try:
                                            paraGraphNum.index(i+1)
                                        except:
                                            if not (startPos.index(k.start())+1) not in paraGraphNum:
                                                   flag=False
                                        else:
                                            if not ((k.start()!=startPos[paraGraphNum.index(i+1)]) or (startPos.index(k.start())+1) not in paraGraphNum):
                                                flag=False
                                    if flag:
                                        paraGraphNum=paraGraphNum + [i+1]
                                        wordDetected=wordDetected + [tempWord]
                                        if (literature_para[i] and (tempWord in properNouns[i])):
                                            suggest=tempWord.title()
                                            suggest='' if suggest==tempWord else suggest
                                            errorCode=errorCode + ['Spelling mistake (Proper Noun)']
                                        else:
                                            US_suggest=list(updatedDict_US_sugg.suggest(tempWord))
                                            GB_suggest=list(h_GB.suggest(tempWord))
                                            US_out=[list(add_words_sugg_all.loc[k.lower()==add_words_sugg_all['key'],'val']) for k in US_suggest if k.lower() in list(add_words_sugg_all['key'])]
                                            US_out=list({i[0] for i in US_out})
                                            GB_out=[list(add_words_sugg_all.loc[k.lower()==add_words_sugg_all['key'],'val']) for k in GB_suggest if k.lower() in list(add_words_sugg_all['key'])]
                                            GB_out=list({i[0] for i in GB_out})
                                            if len(US_out)!=0:
                                                suggest_US=US_out
                                            else:
                                                suggest_US=US_suggest
                                            if len(GB_out)!=0:
                                                suggest_GB=GB_out
                                            else:
                                                suggest_GB=GB_suggest
                                            suggest=suggest_US + suggest_GB
                                            prevWord=replacedWordIndex[i][j-1]
                                            if len(additionalSpellSuggestion[additionalSpellSuggestion['key']==tempWord]) != 0:
                                                if len(unitSpellSuggestion[unitSpellSuggestion['key']==tempWord])!=0:
                                                    if re.search("(?<![A-Za-z0-9.])(\\d*[.]{0,1}\\d+)(?![A-Za-z0-9.])",prevWord):
                                                        suggest = list(set(list(unitSpellSuggestion[unitSpellSuggestion['key']==tempWord]['val'].values) + \
                                                                list(additionalSpellSuggestion[additionalSpellSuggestion['key']==tempWord]['val'].values) + suggest))
                                                    else:
                                                        suggest = list(set(list(additionalSpellSuggestion[additionalSpellSuggestion['key']==tempWord]['val'].values) + \
                                                                        suggest + list(unitSpellSuggestion[unitSpellSuggestion['key']==tempWord]['val'].values)))
                                                    errC = "Spelling mistake (Unit)"
                                                else:
                                                    suggest = list(additionalSpellSuggestion[additionalSpellSuggestion['key']==tempWord]['val'].values) + suggest
                                                    errC = "Spelling mistake (Proper Noun)" if len((input_ann_NNP.doc_id == i+1) & (input_ann_NNP.token == tempWord) & (input_ann_NNP.start == k.start()+1) & (input_ann_NNP.end == k.end()))>0 else "Spelling mistake"
                                            
                                            else:
                                                if len(unitSpellSuggestion[unitSpellSuggestion['key']==tempWord])!=0:
                                                    if re.search("(?<![A-Za-z0-9.])(\\d*[.]{0,1}\\d+)(?![A-Za-z0-9.])",prevWord):
                                                        suggest = list(set(list(unitSpellSuggestion[unitSpellSuggestion['key']==tempWord]['val'].values) + suggest))
                                                    else:
                                                        suggest = list(set(suggest + list(unitSpellSuggestion[unitSpellSuggestion['key']==tempWord]['val'].values)))
                                                    errC = "Spelling mistake (Unit)"
                                                else:
                                                    suggest = list(set(suggest))
                                                    errC = "Spelling mistake (Proper Noun)" if len((input_ann_NNP.doc_id == i+1) & (input_ann_NNP.token == tempWord) & (input_ann_NNP.start == k.start()+1) & (input_ann_NNP.end == k.end()))>0 else "Spelling mistake"
                                            
                                            for s in suggest:
                                                if re.search("\\b([A-Z]{2,})\\b",s):
                                                    errC = errC + "/ Abbreviation Error"
                                                if re.search("\\w \\w",s):
                                                    errC = errC + "/ No Single spacing"
                                            errorCode = errorCode + [errC]
                                            suggestedWord=suggestedWord + ['/'.join(suggest)]
                                            startPos=startPos + [k.start()]
                                            endPos=endPos + [k.end()]
                                            operation=operation + [wordlen.index(inputData[i][k.start():k.end()])+1]
                                            prevErrorEnd=k.end()
                                            category=category + ['Mandatory']
                                            frontendAction=frontendAction + ['Replace']

        
        ### ABREV as Spell/Unit error ###
        IU_unit_pat="\\b(MIU|IU|KIU)\\b|\\b(([MmKk][.]\\s*){0,1}[Ii][.]\\s*[Uu][.]*)"
        IU_unit_sugg={'IU':"[iU]/ iU",'KIU':"k[iU]/ kiU",'MIU':"M[iU]/ MiU/ m[iU]/ miU"}
        for k in range(len(inputData)):
            m=re.findall(IU_unit_pat,inputData[k],flags=re.IGNORECASE)
            replacedWordIndex[k]=m
        if len(replacedWordIndex)>0:
            for t in range(len(replacedWordIndex)):
                if len(replacedWordIndex[t])!=0:
                    for j in range(len(replacedWordIndex[t])):
                        tempWord=replacedWordIndex[t][j]
                        for s in tempWord:
                            k=s.upper()
                            sugg=IU_unit_sugg.get(re.sub(r'[.]|\s','',k))
                            if sugg is not None:
                                paraGraphNum=paraGraphNum + [t+1]
                                wordDetected=wordDetected + [s]
                                suggestedWord=suggestedWord + [sugg]
                                errorCode=errorCode + ['Spelling mistake (Unit)']
                                category=category + ['Mandatory']
                                startPos=startPos + [re.search(s,inputData[t]).span()[0]]
                                endPos=endPos + [re.search(s,inputData[t]).span()[1]]
                                operation=operation + [j+1]
                                frontendAction=frontendAction + ['Replace']

        
        ### 5. Special Char ###
        # 5.1 disallowed chars
        for j in range(len(inputData)):
            replacedWord=re.finditer("([^ !#$%&'()*+,\\-\\./0-9:;<=>?@A-Za-z\\[\\\\\\]^_`{\\|}])|((?<=[A-Za-z])'(?![A-Za-z]))|((?<![A-Za-z])'(?=[A-Za-z]))", inputData[j])
            for k in replacedWord:
                paraGraphNum=paraGraphNum + [j+1]
                wordDetected=wordDetected + [k.group(0)]
                suggest=' '
                suggestedWord=suggestedWord + [suggest]
                errorCode = errorCode +['Disallowed special char used.']
                category=category + ['Mandatory']
                startPos=startPos + [k.start()]
                endPos=endPos + [k.end()]
                operation=operation + ['specialChar']
                frontendAction=frontendAction + ['Replace']

        # 5.2 Allowed char with extra spacing       
        for p in range(len(inputData)):
            replacedWord=re.finditer('(?<=[a-z)}\\]])[ ]*[,;:](?=[a-zA-Z0-9])|(?<=[a-z)}\\]])[ ]*[.!?](?=[A-Za-z][A-Za-z0-9])]',inputData[p])
            for s in replacedWord:
                paraGraphNum=paraGraphNum + [p+1]
                wordDetected=wordDetected + [s.group(0)]
                suggestedWord=suggestedWord + ['{} '.format(x) for x in [s.group(0)]]
                errorCode = errorCode +['Allowed special char (Missing space-after)']
                category=category + ['Mandatory']
                startPos=startPos + [s.start()]
                endPos=endPos + [s.end()]
                operation=operation + ['specialChar2']
                frontendAction=frontendAction + ['Replace']
            
        # 5.3 Allowed char with no-spacing allowed (underscore)
        for j in range(len(inputData)):
            replacedWord=re.finditer('([_]+\\s+)|(\\s+[_]+)|(\\s+[_]+\\s+)]',inputData[j])
            for k in replacedWord:
                paraGraphNum=paraGraphNum + [j+1]
                wordDetected=wordDetected + [k.group(0)]
                #suggestedWord=suggestedWord + [x.replace(' ', '') for x in [k.group(0)]]
                suggestedWord=suggestedWord + ["_"]
                errorCode = errorCode +['Allowed special char (Unwanted Spacing)']
                category=category + ['Mandatory']
                startPos=startPos + [k.start()]
                endPos=endPos + [k.end()]
                operation=operation + ['specialChar2']
                frontendAction=frontendAction + ['Replace']
    
    
        # 5.4 Allowed char with repetition
        for j in range(len(inputData)):
            replacedWord=re.finditer('([.]{2,})|([!,/:;?@\\\\^_`\\|]{2,})|([#&*%$]{2,})|([-+]{3,})|([<=>]{3,})',inputData[j])
            for k in replacedWord:
                paraGraphNum=paraGraphNum + [j+1]
                wordDetected=wordDetected + [k.group(0)]
                suggestedWord=suggestedWord + [x[0] for x in [k.group(0)]]
                errorCode = errorCode +['Allowed special char (Repetition)']
                category=category + ['Mandatory']
                startPos=startPos + [k.start()]
                endPos=endPos + [k.end()]
                operation=operation + ['specialChar2']
                frontendAction=frontendAction + ['Replace']
    
        # 5.5 Mismatched brackets
        in_data = ['{','[','(']
        out_data = ['}',']',')']
        def check_match(statements):
            stack = []
            for ch in statements:
                if ch in in_data:
                    stack.append(ch)
                if ch in out_data:
                    last = None
                    if stack: 
                        last = stack.pop()
                    if last == '{' and ch == '}':
                        continue
                    elif last == '[' and ch == ']':
                        continue
                    elif last == '(' and ch == ')':
                        continue
                    else:
                        return False
            if len(stack) > 0:
                return False
            else:
                return True

        # Getting the line index of unmatched parentheses
        line_list = list(map(check_match, inputData))
        line_index = [index+1 for index,value in enumerate(line_list) if value==False]
        line_iter=iter(line_index)

        # Predefined function to get index of unmatched parentheses
        def badBrackets(S):
            deltas   = [(c=='{')-(c=='}') for c in S] # 1 or -1 for open/close
            forward  = [*accumulate(deltas,lambda a,b:max(0,a)+b)]        # forward levels
            backward = [*accumulate(deltas[::-1],lambda a,b:min(0,a)+b)]  # backward levels
            levels   = [min(f,-b) for f,b in zip(forward,backward[::-1])] # combined levels
            return [i for i,b in enumerate(levels) if b<0]                # mismatches

        # Getting the start index in list
        start_index1 = list(map(badBrackets, inputData))
        # Repetition for all types of braces
        def badBrackets2(S):
            deltas   = [(c=='(')-(c==')') for c in S] # 1 or -1 for open/close
            forward  = [*accumulate(deltas,lambda a,b:max(0,a)+b)]        # forward levels
            backward = [*accumulate(deltas[::-1],lambda a,b:min(0,a)+b)]  # backward levels
            levels   = [min(f,-b) for f,b in zip(forward,backward[::-1])] # combined levels
            return [i for i,b in enumerate(levels) if b<0]                # mismatches
        start_index2 = list(map(badBrackets2, inputData))
        def badBrackets3(S):
            deltas   = [(c=='[')-(c==']') for c in S] # 1 or -1 for open/close
            forward  = [*accumulate(deltas,lambda a,b:max(0,a)+b)]        # forward levels
            backward = [*accumulate(deltas[::-1],lambda a,b:min(0,a)+b)]  # backward levels
            levels   = [min(f,-b) for f,b in zip(forward,backward[::-1])] # combined levels
            return [i for i,b in enumerate(levels) if b<0]                # mismatches
        start_index3 = list(map(badBrackets3, inputData))
        # Merging the start indices
        start_index = [start_index1[i] + start_index2[i] + start_index3[i] for i in range(len(start_index1))]
        # Predefined function to get the unmatched parenthesis
        def findElements(lst1, lst2):
             return [lst1[i] for i in lst2]
        word_Detected = list(map(findElements, inputData, start_index))
        # Converting nested list to normal list
        word_Detected = [val for sublist in word_Detected for val in sublist]
        # Converting to dataframe afrom list and renaming the columns
        wordDetected_df = pd.DataFrame({'col':word_Detected})
        wordDetected_df.rename(columns = {'col':'Error'}, inplace = True)
        # Getting the length of nested start_index list
        count = list(map(len, start_index))
        # Repeating the elements of line_index as per the length of nested list
        output =  list(chain.from_iterable(repeat(value, count) for value, count in [(next(line_iter),count[i])for i in range(len(count)) if count[i]!=0]))
        #print(output)
        # Converting to dataframe and renaming the column
        paragraph_num_df = pd.DataFrame({'col':output})
        paragraph_num_df.rename(columns = {'col':'ParagraphNum'}, inplace = True)
        # Converting nested list to normal list of start index
        start_index = [val for sublist in start_index for val in sublist]
        # Converting to dataframe and renaming the column
        start_index_df = pd.DataFrame({'col':start_index})
        start_index_df.rename(columns = {'col':'StartPos'}, inplace = True)
        # Getting the end indices
        end_index = [x+1 for x in start_index]
        # Converting to dataframe and renaming the column
        end_index_df = pd.DataFrame({'col':end_index})
        end_index_df.rename(columns = {'col':'EndPos'},inplace = True)
        # Concating the dataset
        dataset = pd.concat([paragraph_num_df,wordDetected_df],axis=1)
        # Adding the other necessary fields
        dataset['Suggestion'] = ""
        dataset['ErrorType'] = 'Unmatched parenthesis/ brace/ bracket.'
        dataset['Category'] = 'Mandatory'
        dataset_final = pd.concat([dataset,start_index_df,end_index_df],axis=1)
        dataset_final.insert(7,'Operation','Unmatched parenthesis')
        dataset_final.insert(8,'FrontendAction','Replace')
        
        ### 6. consecutive repetition ###
        # function to get first repeated phrase
        def firstRepeatedPhrase(sentence):
            # splitting the string
            word_lis = list(sentence.split(" "))
            # Calculating frequency of every word
            frequency = Counter(word_lis)
            # Traversing the list of words
            rep = []
            for i in word_lis:
                # checking if frequency is greater than 1
                if(frequency[i] > 1):
                    if i not in rep:
                        rep.append(i)
            return [(" ").join(rep)]
            
        for j in range(len(inputData)):
            repeatedWordindex = re.finditer(r"\b(.+)(\s+\1\b)+",inputData[j].lower())
            for p in repeatedWordindex:
                if len(p.group()) != 0:
                    start = p.start()
                    end = p.end()
                    word = inputData[j][start:end]
                    if word.count(" ") != len(word):
                        paraGraphNum=paraGraphNum + [j+1]
                        wordDetected=wordDetected + [word]
                        errorCode = errorCode +["Repetitive Information"]
                        category=category + ['Optional']
                        startPos=startPos + [start]
                        endPos=endPos + [end]
                        operation=operation + ['repeat']
                        frontendAction=frontendAction + ['Replace']
                        Suggestion = firstRepeatedPhrase(p.group(0))
                        suggestedWord=suggestedWord + Suggestion


        ### 7. Extra spacing ###
        # Getting the start index
        start_index=[] 
        for i in inputData: 
            start_index.append([m.start() for m in re.finditer("\s+\s+", i)])
        # Getting to single list from nested list and framing to dataframe
        start_index = [val for sublist in start_index for val in sublist]
        start_index_df = pd.DataFrame({'col':start_index})
        start_index_df.rename(columns = {'col':'StartPos'}, inplace = True)
        # Extracting the end index list and framing to dataframe
        end_index=[]
        for i in inputData:
            end_index.append([m.end() for m in re.finditer("\s\s+", i)])
        end_index = [val for sublist in end_index for val in sublist]
        end_index_df = pd.DataFrame({'col':end_index})
        end_index_df.rename(columns = {'col':'EndPos'},inplace = True)
        # Adding operation column
        def label_row(row):
            if  (row['EndPos']!=None) :
                return "extra_spacing"
            else:
                ''
        end_index_df['Operation'] = end_index_df.apply(lambda row: label_row(row), axis=1)
        # Initializing the list
        worddetected=suggestedword=[]
        paraNum=[]
        Error_Type = []
        action = []
        # Getting the remaining fields
        for j in range(len(inputData)):
                replacedWord=re.finditer("\s\s+",inputData[j])
                for k in replacedWord:
                    if  re.search(r'^[A-Z]+$',k.group(0)):
                        next
                    paraNum=paraNum + [j+1]
                    worddetected=worddetected + [k.group(0)]
                    suggest=' '
                    suggestedword=suggestedword + [suggest]
                    Error_Type = Error_Type +["Extra spacing found."]
                    action = action + ['Replace']
        paraGraphNum_df = pd.DataFrame({'col':paraNum})
        paraGraphNum_df.rename(columns = {'col':'ParagraphNum'}, inplace = True)
        suggestedWord_df = pd.DataFrame({'col':suggestedword})
        suggestedWord_df.rename(columns = {'col':'Suggestion'}, inplace = True)
        wordDetected_df = pd.DataFrame({'col':worddetected})
        wordDetected_df.rename(columns = {'col':'Error'}, inplace = True)
        Error_Type_df = pd.DataFrame({'col':Error_Type})
        Error_Type_df.rename(columns = {'col':'ErrorType'}, inplace = True)
        action_df = pd.DataFrame({'col':action})
        action_df.rename(columns = {'col':'FrontendAction'}, inplace = True)
        def label_row(row):
            if  (row['ErrorType']!=None) :
                return "Mandatory"
            else:
                ''
        Error_Type_df['Category'] = Error_Type_df.apply(lambda row: label_row(row), axis=1)
        dataset = [paraGraphNum_df,wordDetected_df,suggestedWord_df,\
                  Error_Type_df,start_index_df,end_index_df, action_df]  # List of your dataframes
        extraspace_df = pd.concat(dataset,axis=1)

        
        ### 11. Wrong date ###
        # define a function to check if the date format is valid
        def convertDate(dateString = "", ddmm = False):
            # if date is empty 
            if dateString == None or dateString == "" or dateString == np.nan:
                return [None, "Invalid Date String"]
            # for non-empty date
            else:
                day = "01"
                day_fs = "%d"
                reason = "Valid"
                ### case 1 Mmm DD, YYYY (eg. Jun 1, 2019;  Jun 1, 19) ###
                date_1 = r"^\b([A-Za-z]{3,9}[ ]{0,2}[-/]{0,1}[ ]{0,2}[0-9]{1,2}[ ]{0,2}[,][ ]{0,2}[1-2][0-9]{1,3})\b$"
                exp = re.compile(date_1, flags=re.IGNORECASE)
                if exp.search(dateString):
                    mon = re.findall("[A-Za-z]+", dateString)[0]          # extract month
                    mon_fs = "%b" if len(mon)==3 else "%B"                # set month format
                    temp = re.findall("[0-9]+", dateString)               # extract day and year
                    day = temp[0]                                         # set day
                    year = temp[1]                                        # set year
                    year_fs = "%Y" if len(year)==4 else "%y"              # set year format
                    # convert date for case 1
                    try:
                        dateValue = str(datetime.strptime(str(day + "," + mon + "," + year), str(day_fs + "," + mon_fs + "," + year_fs)).strftime('%Y-%m-%d'))
                    except ValueError:
                        pass
                        dateValue = None
                    # if conversion can not be performed, identify wether day or month is invalid    
                    if pd.isnull(dateValue):
                        # for invalid day
                        try:
                            d = datetime.strptime(str("01" + "," + mon + "," + year), str("%d" + "," + mon_fs + "," + year_fs)).strftime('%Y-%m-%d')
                            invDay = pd.notnull(d)
                        except ValueError:
                            pass
                            d = None
                            invDay = pd.notnull(d)
                        # for invalid month   
                        try: 
                            m = datetime.strptime(str(day + "," + "Jan" + "," + year), str(day_fs + "," + "%b" + "," + year_fs)).strftime('%Y-%m-%d')
                            invMon = pd.notnull(m)
                        except ValueError:
                            m = None
                            invMon = pd.notnull(m)
                        # reason for wrong date format
                        if(invDay&invMon | (not invDay)&(not invMon)):
                            reason = "Invalid DAY and/or MONTH"
                        elif invDay == True: # or invMon == False:
                            reason = "Invalid " + "DAY" + " provided"
                        elif invMon == True: #or invDay == False:
                            reason = "Invalid " + "MONTH" + " provided"
                    return [dateValue, reason]

                ### case 2  D M Y or M D Y (eg. ddmm=T: 31/1/2019; 31-1-2019; ddmm=F: 01-31-2019) 
                date_2 = r"^\b([0-9]{1,2}[ ]{0,2}[-/.][ ]{0,2}[0-9]{1,2}[ ]{0,2}[-/.][ ]{0,2}[0-9]{2,4})\b$"
                exp = re.compile(date_2, flags=re.IGNORECASE)
                if exp.search(dateString):
                    mon_fs = "%m"
                    temp = re.findall("[0-9]+", dateString)               # extract day, month and year
                    year = temp[2]
                    year_fs = "%Y" if len(year)==4 else "%y"              # set year format
                    if ddmm == True:                                      # try dd-mm-YY format only
                        day = temp[0]                                     # 1st value in 'temp' is day
                        mon = temp[1]                                     # 2nd value in 'temp' is month 
                        # convert date for case 2 and ddmm==true
                        try:
                            dateValue = str(datetime.strptime(str(day + "," + mon + "," + year), str(day_fs + "," + mon_fs + "," + year_fs)).strftime('%Y-%m-%d'))
                        except ValueError:
                            pass
                            dateValue = None
                        if pd.isnull(dateValue):
                            rea_func = lambda x,y : "DAY and MONTH" if int(x)>31 and int(y)>12 else ("MONTH" if int(y)>12 else "DAY")
                            reason = "Invalid " + rea_func(day, mon) + " provided (dd-mm-yyyy)"
                    else:                                                 # try, mm-dd then dd-mm                                      
                        if int(temp[0])<=12 and int(temp[1])<=31:         # mm-dd (1st choice)
                            mon = temp[0]
                            day = temp[1]
                        elif int(temp[0])<=31 and int(temp[1])<=12:       # dd-mm (2nd choice)        
                            day = temp[0]
                            mon = temp[1]
                        else:
                            reason = lambda x,y : "DAY and MONTH" if int(x)>31 and int(y)>31 else ("MONTH" if int(x)>12 and int(y)>12 else "DAY") 
                            return (None, "Invalid " + reason(temp[0], temp[1]) + " provided") 
                        # convert date for case 2 and ddmm==false
                        try:
                            dateValue = str(datetime.strptime(str(day + "," + mon + "," + year), str(day_fs + "," + mon_fs + "," + year_fs)).strftime('%Y-%m-%d'))
                        except ValueError:
                            pass
                            dateValue = None
                        if pd.isnull(dateValue):
                            reason = "Invalid MONTH provided"
                    return [dateValue, reason]  

                ### case 2.2 Y M D or Y D M (eg. ddmm=T: 1932/23/2 to 1932-02-23; ddmm=F: 1932/1/23 to 1932-01-23) ###
                date_2_2 = r"^\b([0-9]{4}[ ]{0,2}[-/.][ ]{0,2}[0-9]{1,2}[ ]{0,2}[-/.][ ]{0,2}[0-9]{1,2})\b$"
                exp = re.compile(date_2_2, flags=re.IGNORECASE)
                if exp.search(dateString):
                    mon_fs = "%m"
                    temp = re.findall("[0-9]+", dateString)               # extract day, month and year
                    year = temp[0]
                    year_fs = "%Y" if len(year)==4 else "%y"              # set year format
                    if ddmm == True:                                      # try dd-mm-YY format only
                        day = temp[1]                                     # 2nd value in 'temp' is day
                        mon = temp[2]                                     # 3rd value in 'temp' is month 
                        # convert date for case 2.2 and ddmm==true
                        try:
                            dateValue = str(datetime.strptime(str(day + "," + mon + "," + year), str(day_fs + "," + mon_fs + "," + year_fs)).strftime('%Y-%m-%d'))
                        except ValueError:
                            pass
                            dateValue = None
                        if pd.isnull(dateValue):
                            rea_func = lambda x,y : "DAY and MONTH" if int(x)>31 and int(y)>12 else ("MONTH" if int(y)>12 else "DAY")
                            reason = "Invalid " + rea_func(day, mon) + " provided (dd-mm-yyyy)"
                    else:                                                 # try, mm-dd then dd-mm                                      
                        if int(temp[1])<=12 and int(temp[2])<=31:         # mm-dd (1st choice)
                            mon = temp[1]
                            day = temp[2]
                        elif int(temp[1])<=31 and int(temp[2])<=12:       # dd-mm (2nd choice)        
                            day = temp[1]
                            mon = temp[2]
                        else:
                            reason = lambda x,y : "DAY and MONTH" if int(x)>31 and int(y)>31 else ("MONTH" if int(x)>12 and int(y)>12 else "DAY") 
                            return (None, "Invalid " + reason(temp[1], temp[2]) + " provided")
                        # convert date for case 2.2 and ddmm==false
                        try:
                            dateValue = str(datetime.strptime(str(day + "," + mon + "," + year), str(day_fs + "," + mon_fs + "," + year_fs)).strftime('%Y-%m-%d'))
                        except ValueError:
                            pass
                            dateValue = None
                        
                        if pd.isnull(dateValue):
                            reason = "Invalid MONTH provided"
                    return [dateValue, reason]
            
                ### case 3 D Mmm YYYY or YYYY Mmm D (eg. 15-Jan-2019 to 2019-01-15, 32/april/25 to 2032-04-25) ###
                date_3 = r"^\b((\d{1,4}[ -/]*[A-Za-z]{3,9}[ -/]*\d{1,4})|([A-Za-z]{3,9}[ -/]*\d{2,4})|([1-2][0-9]{3}))\b$"
                exp = re.compile(date_3, flags=re.IGNORECASE)
                if exp.search(dateString):
                    mon = "Jan"
                    mon_fs = "%b"
                    temp = re.findall("[A-Za-z]+", dateString)                
                    if len(temp) > 0:
                        temp = temp[0]                                        # extract month
                        mon = temp
                        mon_fs = "%b" if len(mon)==3 else "%B"                # set month format
                    else:
                        pass
                    temp = re.findall("[0-9]+", dateString)                   # extract day and year
                    if len(temp)==2:
                        if int(temp[0])<=31:
                            day = temp[0]
                            year_func = lambda x : x if len(x)==4 else x[len(x)-2 : 4]
                            year = year_func(temp[1])
                            year_fs = "%Y" if len(temp[1])==4 else "%y"       # set year format
                        elif int(temp[0])>31 and int(temp[1])<=31 and len(temp[1])<=2:   # year+day
                            year = temp[0]
                            year_fs = "%Y" if len(year)==4 else "%y"                    # set year format
                            day = temp[1]
                            reason = "Considered format: Year-Mon-Day"
                        else:
                            # for invalid month
                            try:
                                m = datetime.strptime(str("01" + "," + mon + "," + "2020"), str("%d" + "," + mon_fs + "," + "%Y")).strftime('%Y-%m-%d')
                                invMon = pd.isnull(m)
                            except ValueError:
                                pass
                                m = None
                                invMon = pd.isnull(m)
                            if invMon == True:
                                return [None, "Invalid DAY and MONTH provided"]
                            else:
                                return [None, "Invalid DAY provided"]
                    elif len(temp)==1:
                        year = temp[0]
                        year_fs = "%Y" if len(temp[0])==4 else "%y"
                    try:
                        dateValue = str(datetime.strptime(str(day + "," + mon + "," + year), str(day_fs + "," + mon_fs + "," + year_fs)).strftime('%Y-%m-%d'))
                    except ValueError:
                        pass
                        dateValue = None
                    if pd.isnull(dateValue):
                        reason = "Invalid MONTH provided"
                    return [dateValue, reason]
                return [None, "Invalid Date Pattern accepted: Mon DD, Y; DD-Mon-Y; DD Mon Y; DD/Mon/Y; Y-Mon-DD;;; MM-DD-Y; MM/DD/Y; DD-MM-Y; DD/MM/Y"]

        # file containing different spellings for month  
        mon_equival_rds = pyreadr.read_r("./mon_equival.rds")
        mon_equival = mon_equival_rds[None]
        # CASE 1
        date_patterns = [r"\b([A-Za-z]{3,}[ -/][0-9]{1,2}[ ]{0,2}[,][ ]{0,2}(19|20)\d{2})\b", 
                         r"\b(\d{1,2}[ -/]*[A-Za-z]{3,}[ -/]*(19|20)\d{2})\b|\b([A-Za-z]{3,}[ -/]*(19|20)\d{2})\b",
                         r"\b((19|20)\d{2}[ -/]*[A-Za-z]{3,}[ -/]*\d{1,2})\b|\b((19|20)\d{2}[ -/]*[A-Za-z]{3,})\b"] 
        date_1_1 = "|".join(date_patterns)
        # get start and end position of extracted dates
        index_of_occ_1 = []
        for j in range(len(inputData)):
            line = []
            for i in re.finditer(date_1_1, inputData[j]):
                start, end = i.start(), i.end()
                line.append((start, end))
            index_of_occ_1.append(line)
        # check the format of extracted dates and identify the errors in date patterns 
        for p in range(len(index_of_occ_1)):
            if len(index_of_occ_1[p]) > 0:
                for i in range(len(index_of_occ_1[p])):
                    dt_str = inputData[p][index_of_occ_1[p][i][0]:index_of_occ_1[p][i][1]] #extracted dates 
                    #print(dt_str)
                    validExistance = convertDate(dt_str)   # convertDate is a customized function
                    if pd.notna(validExistance[0]):
                        next
                    exp = re.compile("DAY.*MONTH")
                    exp1 = re.compile("DAY")
                    exp2 = re.compile("MONTH")
                    if exp.search(validExistance[1]):
                        mon_str = re.findall("[A-Za-z]+", dt_str)[0]
                        try:
                            mon_str2 = mon_equival[mon_equival['mon'].str.contains(r"\b"+re.escape(mon_str)+r"\b", flags=re.IGNORECASE) == True]['equiv'].values[0]
                        except IndexError:
                            pass
                            mon_str2 = None
                        if pd.isna(mon_str2):
                            continue
                        dt_str2 = re.sub(mon_str, mon_str2, dt_str)
                        dt_str2 = re.sub("(?<!\d)(\d{1,2})(?!\d)", "01" ,dt_str2)
                        if pd.notna(mon_str2) and pd.notna(convertDate(dt_str2)[0]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [dt_str2]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day & month)."]
                            category=category + ['Optional']
                            startPos=startPos + [index_of_occ_1[p][i][0]]
                            endPos=endPos + [index_of_occ_1[p][i][1]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['Replace']
                            
                    elif exp1.search(validExistance[1]):
                        dt_str2 = re.sub("(?<!\d)(\d{1,2})(?!\d)", "01", dt_str)
                        if pd.notna(convertDate(dt_str2)[0]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [dt_str2]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day)."]
                            category=category + ['Optional']
                            startPos=startPos + [index_of_occ_1[p][i][0]]
                            endPos=endPos + [index_of_occ_1[p][i][1]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['Replace']
                            
                    elif exp2.search(validExistance[1]):
                        mon_str = re.findall("[A-Za-z]+", dt_str)[0]
                        try:
                            mon_str2 = mon_equival[mon_equival['mon'].str.contains(r"\b"+re.escape(mon_str)+r"\b", flags=re.IGNORECASE) == True]['equiv'].values[0]
                        except IndexError:
                            pass
                            mon_str2 = None
                        if pd.isna(mon_str2):
                            continue
                        dt_str2 = re.sub(mon_str, mon_str2, dt_str)
                        if pd.notna(mon_str2) and pd.notna(convertDate(dt_str2)[0]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [dt_str2]
                            errorCode=errorCode + ["Invalid/ ambiguous date (month)."]
                            category=category + ['Optional']
                            startPos=startPos + [index_of_occ_1[p][i][0]]
                            endPos=endPos + [index_of_occ_1[p][i][1]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['Replace']

        # CASE 2
        date_1_3 = r"\b(\d{1,2}[ -/]*[A-Za-z]{3,}[ -/]*\d{1,3})\b"
        # get start and end position of extracted dates
        index_of_occ_2 = []
        for j in range(len(inputData)):
            line = []
            for i in re.finditer(date_1_3, inputData[j]):
                start, end = i.start(), i.end()
                line.append((start, end))
            index_of_occ_2.append(line)
        # check the format of extracted dates and identify the errors in date patterns 
        for p in range(len(index_of_occ_2)):
            if len(index_of_occ_2[p]) > 0:
                for i in range(len(index_of_occ_2[p])):
                    dt_str = inputData[p][index_of_occ_2[p][i][0]:index_of_occ_2[p][i][1]] #extracted dates 
                    validExistance = convertDate(dt_str)   # convertDate is a customized function
                    exp = re.compile("DAY.*MONTH")
                    exp1 = re.compile("DAY")
                    exp2 = re.compile("MONTH")
                    if pd.notna(validExistance[0]):
                        paraGraphNum=paraGraphNum + [p+1]
                        wordDetected=wordDetected + [dt_str]
                        suggestedWord=suggestedWord + [""]
                        errorCode=errorCode + ["Invalid/ ambiguous date (incomplete year)."]
                        category=category + ['Optional']
                        startPos=startPos + [index_of_occ_2[p][i][0]]
                        endPos=endPos + [index_of_occ_2[p][i][1]]
                        operation=operation + ['date2']
                        frontendAction=frontendAction + ['No Action']
                    elif exp.search(validExistance[1]):
                        mon_str = re.findall("[A-Za-z]+", dt_str)[0]
                        try:
                            mon_str2 = mon_equival[mon_equival['mon'].str.contains(r"\b"+re.escape(mon_str)+r"\b", flags=re.IGNORECASE) == True]['equiv'].values[0]
                        except IndexError:
                            pass
                            mon_str2 = None
                        if pd.isna(mon_str2):
                            continue
                        digitPart = re.findall("\d+", dt_str)
                        dt_str2 = "01-" + mon_str2 + "-" + digitPart[1]
                        dt_str3 = digitPart[0] + "-" + mon_str2 + "-01"
                        if pd.notna(convertDate(dt_str2)[0]) or pd.notna(convertDate(dt_str3)[0]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [dt_str2 + " OR " + dt_str3]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day, month, incomplete year)."]
                            category=category + ['Optional']
                            startPos=startPos + [index_of_occ_2[p][i][0]]
                            endPos=endPos + [index_of_occ_2[p][i][1]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['No Action']
                    elif exp1.search(validExistance[1]):
                        mon_str = re.findall("[A-Za-z]+", dt_str)[0]
                        digitPart = re.findall("\d+", dt_str)
                        dt_str2 = "01-" + mon_str + "-" + digitPart[1]
                        dt_str3 = digitPart[0] + "-" + mon_str + "-01"
                        if pd.notna(convertDate(dt_str2)[0]) or pd.notna(convertDate(dt_str3)[0]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [dt_str2 + " OR " + dt_str3]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day)."]
                            category=category + ['Optional']
                            startPos=startPos + [index_of_occ_2[p][i][0]]
                            endPos=endPos + [index_of_occ_2[p][i][1]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['No Action']
                    elif exp2.search(validExistance[1]):
                        mon_str = re.findall("[A-Za-z]+", dt_str)[0]
                        try:
                            mon_str2 = mon_equival[mon_equival['mon'].str.contains(r"\b"+re.escape(mon_str)+r"\b", flags=re.IGNORECASE) == True]['equiv'].values[0]
                        except IndexError:
                            pass
                            mon_str2 = None
                        if pd.isna(mon_str2):
                            continue
                        dt_str2 = re.sub(mon_str, mon_str2, dt_str)
                        if pd.notna(mon_str2) and pd.notna(convertDate(dt_str2)[0]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [dt_str2]
                            errorCode=errorCode + ["Invalid/ ambiguous date (month, incomplete year)."]
                            category=category + ['Optional']
                            startPos=startPos + [index_of_occ_2[p][i][0]]
                            endPos=endPos + [index_of_occ_2[p][i][1]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['Replace']

        # CASE 3
        date_1_4 = r"(?<!(\d{1,4}[ -/.]{1,3}))\b([A-Za-z]{3,}[ -/]*\d{1,3})\b(?!([ -/.]{1,3}\d{2,4}))"
        # get start and end position of extracted dates
        index_of_occ_3 = []
        for j in range(len(inputData)):
            line = []
            for i in re.finditer(date_1_4, inputData[j]):
                start, end = i.start(), i.end()
                line.append((start, end))
            index_of_occ_3.append(line)
        # check the format of extracted dates and identify the errors in date patterns 
        for p in range(len(index_of_occ_3)):
            if len(index_of_occ_3[p]) > 0:
                for i in range(len(index_of_occ_3[p])):
                    dt_str = inputData[p][index_of_occ_3[p][i][0]:index_of_occ_3[p][i][1]] #extracted dates 
                    validExistance = convertDate(dt_str)   # convertDate is a customized function
                    mon_str = re.findall("[A-Za-z]+", dt_str)[0]
                    try:
                        mon_str2 = mon_equival[mon_equival['mon'].str.contains(r"\b"+re.escape(mon_str)+r"\b", flags=re.IGNORECASE) == True]['equiv'].values[0]
                    except IndexError:
                        pass
                        mon_str2 = None
                    if pd.isna(mon_str2):
                        continue
                    dt_str2 = re.sub(mon_str, mon_str2, dt_str)
                    if pd.notna(mon_str2) or pd.notna(validExistance[0]):
                        paraGraphNum=paraGraphNum + [p+1]
                        wordDetected=wordDetected + [dt_str]
                        suggest = "" if pd.notna(validExistance[0]) else dt_str2
                        suggestedWord=suggestedWord + [suggest]
                        error = "Invalid/ ambiguous date (incomplete year)." if pd.notna(validExistance[0]) else "Invalid/ ambiguous date (month, incomplete year)."
                        errorCode=errorCode + [error]
                        category=category + ['Optional']
                        startPos=startPos + [index_of_occ_3[p][i][0]]
                        endPos=endPos + [index_of_occ_3[p][i][1]]
                        operation=operation + ['date2']
                        action = 'No Action' if suggest == "" else 'Replace'
                        frontendAction=frontendAction + [action]

        # CASE 4
        date_patterns = [r"\b([0-9]{1,2}[ ]{0,2}[.][ ]{0,2}[0-9]{1,2}[ ]{0,2}[.][ ]{0,2}\d{4})\b", 
                     r"\b(\d{4}[ ]{0,2}[.][ ]{0,2}[0-9]{1,2}[ ]{0,2}[.][ ]{0,2}[0-9]{1,2})\b",
                    r"\b([0-9]{1,2}[ ]{0,2}[-][ ]{0,2}[0-9]{1,2}[ ]{0,2}[-][ ]{0,2}\d{4})\b",
                    r"\b(\d{4}[ ]{0,2}[-][ ]{0,2}[0-9]{1,2}[ ]{0,2}[-][ ]{0,2}[0-9]{1,2})\b",
                    r"\b([0-9]{1,2}[ ]{0,2}[/][ ]{0,2}[0-9]{1,2}[ ]{0,2}[/][ ]{0,2}\d{4})\b",
                    r"\b(\d{4}[ ]{0,2}[/][ ]{0,2}[0-9]{1,2}[ ]{0,2}[/][ ]{0,2}[0-9]{1,2})\b"] 
        date_2 = re.compile("|".join(date_patterns), re.IGNORECASE)
        # get start and end position of extracted dates
        index_of_occ_4 = []
        for j in range(len(inputData)):
            line = []
            for i in re.finditer(date_2, inputData[j]):
                start, end = i.start(), i.end()
                line.append((start, end))
            index_of_occ_4.append(line)
        # check the format of extracted dates and identify the errors in date patterns 
        if len(index_of_occ_4)>0:
            for p in range(len(index_of_occ_4)):
                if len(index_of_occ_4[p])!=0:
                    start = [index_of_occ_4[p][i][0] for i in range(len(index_of_occ_4[p]))]   
                    end = [index_of_occ_4[p][i][1] for i in range(len(index_of_occ_4[p]))]
                    for i in range(len(start)):
                        dt_str = inputData[p][start[i]:end[i]]
                        validExistance = convertDate(dt_str)   # convertDate is a customized function
                        if pd.notna(validExistance[0]):
                            next
                        exp = re.compile("DAY.*MONTH")
                        exp1 = re.compile("MONTH")
                        exp2 = re.compile("DAY")
                        if exp.search(validExistance[1]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [""]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day, month)."]
                            category=category + ['Optional']
                            startPos=startPos + [start[i]]
                            endPos=endPos + [end[i]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['No Action']
                        elif exp1.search(validExistance[1]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [""]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day/ month)."]
                            category=category + ['Optional']
                            startPos=startPos + [start[i]]
                            endPos=endPos + [end[i]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['No Action']
                        elif exp2.search(validExistance[1]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [""]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day)."]
                            category=category + ['Optional']
                            startPos=startPos + [start[i]]
                            endPos=endPos + [end[i]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['No Action']
        
        # CASE 5
        date_patterns = [r"\b([0-9]{1,2}[ ]{0,2}[.][ ]{0,2}[0-9]{1,2}[ ]{0,2}[.][ ]{0,2}\d{2})\b", 
                     r"\b([0-9]{1,2}[ ]{0,2}[-][ ]{0,2}[0-9]{1,2}[ ]{0,2}[-][ ]{0,2}\d{2})\b", 
                     r"\b([0-9]{1,2}[ ]{0,2}[/][ ]{0,2}[0-9]{1,2}[ ]{0,2}[/][ ]{0,2}\d{2})\b"]
        date_2 = re.compile("|".join(date_patterns), re.IGNORECASE)
        # get start and end position of extracted dates
        index_of_occ_5 = []
        for j in range(len(inputData)):
            line = []
            for i in re.finditer(date_2, inputData[j]):
                start, end = i.start(), i.end()
                line.append((start, end))
            index_of_occ_5.append(line)
        # check the format of extracted dates and identify the errors in date patterns     
        if len(index_of_occ_5)>0:
            for p in range(len(index_of_occ_5)):
                if len(index_of_occ_5[p])!=0:
                    start = [index_of_occ_5[p][i][0] for i in range(len(index_of_occ_5[p]))]
                    end = [index_of_occ_5[p][i][1] for i in range(len(index_of_occ_5[p]))]
                    for i in range(len(start)):
                        dt_str = inputData[p][start[i]:end[i]]
                        validExistance = convertDate(dt_str)   # convertDate is a customized function
                        exp = re.compile("DAY.*MONTH")
                        exp1 = re.compile("MONTH")
                        exp2 = re.compile("DAY")
                        if pd.notna(validExistance[0]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [""]
                            errorCode=errorCode + ["Invalid/ ambiguous date (incomplete year)."]
                            category=category + ['Optional']
                            startPos=startPos + [start[i]]
                            endPos=endPos + [end[i]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['No Action']
                        elif exp.search(validExistance[1]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [""]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day, month, incomplete year)."]
                            category=category + ['Optional']
                            startPos=startPos + [start[i]]
                            endPos=endPos + [end[i]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['No Action']
                        elif exp1.search(validExistance[1]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [""]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day/ month, incomplete year)."]
                            category=category + ['Optional']
                            startPos=startPos + [start[i]]
                            endPos=endPos + [end[i]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['No Action']
                        elif exp2.search(validExistance[1]):
                            paraGraphNum=paraGraphNum + [p+1]
                            wordDetected=wordDetected + [dt_str]
                            suggestedWord=suggestedWord + [""]
                            errorCode=errorCode + ["Invalid/ ambiguous date (day, incomplete year)."]
                            category=category + ['Optional']
                            startPos=startPos + [start[i]]
                            endPos=endPos + [end[i]]
                            operation=operation + ['date2']
                            frontendAction=frontendAction + ['No Action']

                            
        ### 12.a Abbreviation ###
        # read the excel containing vaccine names
        vaccineList = pd.read_excel("./Vaccine List.xlsx", sheet_name='Sheet1')
        # open the text file containing excepted abbreviations
        with open('./Abbrev_Exceptions_DrugName.txt') as f:
            accepted_abbrev = f.readlines()
        # remove the empty lines
        accepted_abbrev = [i.replace('\n','') for i in accepted_abbrev]
        # (Look-ahead  ABBREV  Look-behind) pattern
        patterns = [r"(?<!(\b([A-Z]{2,30})[\s\-/,]{1,4}|\b([0-9]{2,30})\s{0,4}[-,]\s{0,4}|\"))", 
                r"\b([A-Z]{2,})\b", 
                r"(?!([\s\-/,]{1,4}([A-Z]{1,30})\b|\s{0,4}[-,]\s{0,4}([0-9]{2,30})\b|\"))"]
        abbrev_pat = "".join(patterns)
        # get start and end position of abbreviations
        index_of_occ = []
        for j in range(len(inputData)):
            line = []
            for i in re.finditer(abbrev_pat, inputData[j]):
                start, end = i.start(), i.end()
                line.append((start, end))
            index_of_occ.append(line)
        # function to identifiy abbreviations based on different conditions
        # set the empty list to store checked abbreviations
        abbrev_processed = []
        # for loop to iterate over each input string
        for p in range(len(index_of_occ)):
            if len(index_of_occ[p]) > 0:                   
                for i in range(len(index_of_occ[p])):
                    # start position of abbreviation
                    st = index_of_occ[p][i][0]                                  
                    # end position of abbreviation
                    ed = index_of_occ[p][i][1]                                  
                    # ABB: actual one (with/without dots)
                    ABB = inputData[p][st:ed]                                   
                    # ABB2: ABB without dots 
                    ABB2 = re.sub("\.", "", ABB)                                
                    # ABB3: ABB with escaped dots, if any
                    ABB3 = re.sub("\.", "\\.", ABB)                             
                    # skip the abbreviations already present in the Exception/ Processed list of abbreviations
                    if ABB2 in abbrev_processed or ABB2 in accepted_abbrev:     
                        continue
                    # check previous word: (Title/ ID/ Study/ Name)
                    prevWord = re.findall(r"\w+[ :-]+$", inputData[p][0:st])
                    exp = re.compile("Title|ID|Study|Program|Survey", re.IGNORECASE)
                    if prevWord != [] and exp.search(prevWord[0]): 
                        continue
                    # check list of vaccine names
                    # skip if at least 5 characters found (enclosed in ()) 
                    if ed-st>=4:
                        exp1 = re.compile(r"("+ABB+r")")
                        if exp1.search(",".join(vaccineList['Description'].astype(str).values)):
                            continue
                    # check for brand name (enclosed in ()/[]) and skip if detected
                    else:
                        brand_prev = re.findall("((?<=\\()[\w ,\\-/]*?)$", inputData[p][0:st])
                        brand_next = re.findall("^([\w ,\\-/]*?(?=\\)))", inputData[p][ed:ed+40])
                        if brand_prev == [] or brand_next == []: 
                            brand_prev = re.findall("((?<=\\[)[\\w ,\\-/]*?)$", inputData[p][0:st])
                            brand_next  = re.findall("^([\\w ,\\-/]*?(?=\\]))", inputData[p][ed:ed+40])
                        if brand_prev != [] and brand_next != []:
                            brand_full_pat = brand_prev[0]+ABB+brand_next[0]
                            brand_full_pat = re.sub("[ -/,]+", "[ -/,]{1,4}", brand_full_pat)
                            brand_full_pat = "\\b("+ brand_full_pat+")\\b"
                            exp2 = re.compile(brand_full_pat, re.IGNORECASE)
                            if exp2.search(",".join(vaccineList[vaccineList['Type'] == "Brand Cleaned"]['Description'].astype(str).values)):
                                continue
                            if exp2.search(",".join(vaccineList[vaccineList['Type'] == "Prod Name"]['Description'].astype(str).values)):
                                continue
                        # check for vaccine name and skip if detected
                        else: 
                            vacc_full_pat = []    
                            vacc_prev = re.findall("(\\w+[ ,\\-/]*?)$", inputData[p][0:st])
                            if vacc_prev != []:
                                vacc_full_pat = [vacc_prev[0]+ABB]
                            vacc_next = re.findall("^([ ,\\-/]*?\\w+)", inputData[p][ed:ed+15]) 
                            if vacc_next != []:
                                vacc_full_pat = vacc_full_pat + [ABB+vacc_next[0]]
                            if len(vacc_full_pat)==0:
                                vacc_full_pat = [ABB]
                            vacc_full_pat = [re.sub("[ -/,]+", "[ -/,]{1,4}", i) for i in vacc_full_pat]
                            vacc_full_pat = "|".join("\\b("+ i+")\\b" for i in vacc_full_pat)
                            exp3 = re.compile(vacc_full_pat, re.IGNORECASE)
                            if exp3.search(",".join(vaccineList[vaccineList['Type'] == "vacCleaned"]['Description'].astype(str).values)):
                                continue
                            if exp3.search(",".join(vaccineList[vaccineList['Type'] == "Prod Name"]['Description'].astype(str).values)):
                                continue
                    # check expansion pattern containing 'ABB2'  
                    ABB2_list = list(zip(list(ABB2),[i.lower() for i in list(ABB2)]))
                    ABB2_pat = ["|".join(ABB2_list[i]) for i in range(len(list(ABB2)))]
                    expansion_pat = "\\b([" + "][\\w']*[\\s\\-,]*(of|for|and|as)*[\\s\\-,]*[".join(ABB2_pat) + \
                    "][\\w']*[\\s\\-,]*)" + "\\([\\s[:punct:]]*" + ABB3 + "[\\s[:punct:]]*\\)"
                    valid_existance = [(i.start(), i.end()) for i in re.finditer(expansion_pat, inputData[p])]
                    if re.findall(expansion_pat, inputData[p]) != []:
                        valid_extr_char_cnt = len(re.findall("\\w", re.findall(expansion_pat, inputData[p])[0]))
                    else:
                        valid_extr_char_cnt = None
                    # validate the abbreviation based on given conditions
                    exp_1 = re.compile("^[VX][IVX]+$")
                    exp_2 = re.compile("^I{2,3}$")
                    exp_3 = re.compile("\\w\\.")
                    exp_4 = re.compile("\\.\\w")
                    if ((valid_existance == [] or not valid_existance[0][0] < st or not ed < valid_existance[0][1] or \
                        valid_extr_char_cnt == None or not valid_extr_char_cnt >= 3*len(ABB2)) and  \
                        not (exp_1.search(ABB) or exp_2.search(ABB)) and \
                        not (exp_3.search(inputData[p][st-2:st]) and exp_4.search(inputData[p][ed:ed+2]))):                
                        paraGraphNum=paraGraphNum + [p+1]
                        wordDetected=wordDetected + [ABB]
                        suggestedWord=suggestedWord + [""]
                        errorCode=errorCode + ["Abbreviation Used"]
                        category=category + ['Optional']
                        startPos=startPos + [st]
                        endPos=endPos + [ed]
                        operation=operation + ["abbrev"]
                        frontendAction=frontendAction + ['No Action']
                    # add to the list of abbreviations
                    abbrev_processed = abbrev_processed + [ABB2]   
         
        
        ### 12.b Dosage Frequency Abbreviations ###
        dosageFreq = pyreadr.read_r("./Dosage_Freq.rds")
        # Converting to dataframe
        dosagefreq = dosageFreq[None]
        # Filtering the data points where case_ignore are True
        dosagefreq['Case_ignore'] = dosagefreq['Case_ignore'].astype(str)
        dosagefreq_true = dosagefreq[(dosagefreq['Case_ignore']=='True')]
        data_range = dosagefreq_true.shape[0]
        # Getting into list under column Code
        dsg_fr = (dosagefreq_true['Code'].values)
        dsg_fr_lower = [x.lower() for x in dsg_fr]
        dsg_fr_capital = [i.title() for i in dsg_fr]
        concat_func = lambda x,y,z: x + "" + y + "" + str(z)
        # Concatenating the lists:Code,lower Code and Capital at the start
        dsg_fr2 = list(map("|".join, zip(dsg_fr,dsg_fr_lower,dsg_fr_capital)))
        # Checking the pattern
        for i in dsg_fr2:
            dsg_pat = r"(?<!(\b([A-Z]{2,30})[\s\-/]{1,4}|\"))\b("+ i+ r")\b(?!(\"|[\s\-/]{1,4}([A-Z]{1,30})\b)"
        # Checking the occurence of error in the Input data
        dosagefreq_true_des = dosagefreq_true['Description'].values
        for i in dsg_fr2:
            dsg_pat = r"(?<!(\b([A-Z]{2,30})[\s\-/]{1,4}|\"))\b("+ i+ r")\b(?!(\"|[\s\-/]{1,4}([A-Z]{1,30})\b))"
            for j in range(len(inputData)):
                replacedWord=re.finditer(dsg_pat,inputData[j])
                for k in replacedWord:
                    paraGraphNum=paraGraphNum + [j+1]
                    wordDetected=wordDetected + [k.group(0)]
                    errorCode = errorCode +['Dosage freq Code used.']
                    suggest=pd.Series([k.group(0)]).map(dict(zip(dsg_fr, dosagefreq_true_des))).tolist()
                    suggestedWord = suggestedWord + suggest
                    startPos=startPos + [k.start()]
                    endPos=endPos + [k.end()]
                    operation=operation + ['dosage']
                    category=category + ['Optional']
                    frontendAction=frontendAction + ['Replace']
        
        os.chdir(curr_dir)
    
        # Appending into dataframe
        wordTable=pd.DataFrame (data={'ParagraphNum':paraGraphNum,'Error':wordDetected,'Suggestion':suggestedWord,'ErrorType':errorCode,
                                   'Category':category,'StartPos':startPos,'EndPos':endPos,'Operation':operation, 
                                  'FrontendAction': frontendAction})
        wordTable=pd.concat([wordTable,dataset_final,extraspace_df],ignore_index=True)
        wordTable['ParagraphNum']=wordTable['ParagraphNum'].astype('int')
        wordTable['StartPos']=wordTable['StartPos'].astype('int')
        wordTable['EndPos']=wordTable['EndPos'].astype('int')
    
        if len(wordTable) == 0:
            return "No Errors Found."
        else:
            return (wordTable.sort_values(by=['ParagraphNum','StartPos']).reset_index(drop=True)).to_dict()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        return f"Internal error: {str(e)}, line_num: {int(exc_tb.tb_lineno)}"