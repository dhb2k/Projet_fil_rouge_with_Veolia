import numpy as np
import pandas as pd

def processing_df(df,id_equipment,drop_null="yes",drop_rendement="yes",r_seuil=20):
    ''' 
    Le jeu de données d'entrée df est le data set brute.
    On lui passe également le numéro de l'équipement
    Le jeu de données ne contient pas la colonne rendement, cette fonction  la crée

    '''
    equipment_tag = df["equipment_id"].unique()[id_equipment]
    elem = equipment_tag.split("_")
    print("Information sur l'équipement sélectionné \n\tBatiment : {0}\n\tEquipement : {1}".format(elem[1],' '.join(elem[2:])))
    
    text_columns = ["building_id","equipment_type","equipment_type_and_sub_type","equipment_id"]
    
    df_temp = df.loc[df["equipment_id"] == df["equipment_id"].unique()[id_equipment]]
    df_temp = (df_temp.drop(text_columns,axis=1))
    
    if drop_null == "yes":
        df_temp = df_temp.loc[df_temp["energy_input_in_mwh"]!=0]
        df_temp = df_temp.loc[df_temp["energy_output_in_mwh"]!=0]
        df_temp["rendement"] = df_temp["energy_output_in_mwh"]/df_temp["energy_input_in_mwh"]
    
    
    if drop_rendement == "yes":
        df_temp = df_temp.loc[df_temp["rendement"]<=r_seuil]
    
    df_temp = df_temp.set_index("timestamp_local")

    return df_temp, equipment_tag



def drift_sample_generator_old_version(max_val,delta,nb_val):
    '''
    Cette fonction permet de générer une suite de point présentant un drift.
    
    max_val : valeur maximale initiale de la courbe. Valeur de reférence pour le drift
    delta : pourcentage de la décroissance
    nb_val : nombre de point

    Créer pour la suite une variante de cette fonction, qui rajoute du bruit sur les données
    drift_sample_generator(max_val,delta,nb_val,var_noise=0.01,noise="no")
    '''
    min_val = max_val*(1-delta)
    x = np.linspace(1,100,num=nb_val)
    y = max_val - np.exp(np.sqrt(x))/np.exp(np.sqrt(x[-1]))*(max_val - min_val)
    return y


def drift_sample_generator(start_time,end_time,max_val,delta):
    '''
    Cette fonction permet de générer une suite de point présentant un drift.
    Le nombre de valeur étant déterminer par les dates de début et de fin
    
    start_time : date initiale
    end_time : date de fin
    max_val : valeur maximale initiale de la courbe. Valeur de reférence pour le drift
    delta : pourcentage de la décroissance
    nb_val : nombre de point

    le programme retourne un dataframe avec en index le temps, au format AAAA-MM-JJ et les valeurs de drift sur cette période

    Créer pour la suite une variante de cette fonction, qui rajoute du bruit sur les données
    drift_sample_generator(max_val,delta,nb_val,var_noise=0.01,noise="no")

    EXAMPLE
    df = drift_sample_generator(start_time = "01/2021",
                               end_time = "03/2021",
                               max_val=0.8,
                               delta=0.2)
    plt.plot(df)

    '''
    # définition de la valeur minimal atteinte sur la période précisée
    min_val = max_val*(1-delta) 

    # Création du dataframe avec les periodes passer en argument
    df = pd.DataFrame({"timestamp":[start_time, end_time],"rendement":[max_val,min_val]})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")

    # Echantillonnage au jour
    df = df.resample("D").max()
    nb_val = df.resample("D").max().shape[0]
    
    x = np.linspace(1,100,num=nb_val)
    df["rendement"] = max_val - np.exp(np.sqrt(x))/np.exp(np.sqrt(x[-1]))*(max_val - min_val)
        
    return df


def saison_detection(df,seuil_ecart=5,bloc_size=10):
    '''
    Ce script permet de détecter les saisonnalités dans une série temporelle
    de dataframe donné en paramètre doit avoir des dates en index !!!
    '''

    saison_index = []
    arr_temp = []

    df = df.sort_index()

    for idx in range(1,df.index.shape[0]):
        if idx == 1:
            arr_temp.append(df.index[idx-1])
        ecart = (df.index[idx] - df.index[idx-1]).days
        if ecart < seuil_ecart:
            arr_temp.append(df.index[idx])
        else:
            if len(arr_temp) >= bloc_size:      
                saison_index.append(arr_temp)
            arr_temp = []
            arr_temp.append(df.index[idx])
    
    if len(arr_temp) >= bloc_size:
        saison_index.append(arr_temp)
    
    return saison_index


class veolia_drift_old():
    def __init__(self,n_period):
        
        self.n = n_period
    def fit(self,df,seuil_ecart=5,min_bloc_size=10):
        temp = []
        self.df = df
        self.seuil_ecart = seuil_ecart
        self.bloc_size = min_bloc_size
        self.saison = saison_detection(self.df,self.seuil_ecart,self.bloc_size)
        for idx in range(len(self.saison)):
            df_saison = self.df.loc[self.saison[idx]].sort_index()
            debut_saison = df_saison.index[0]
            fin_saison = df_saison.index[-1]
            rendement_initial = df_saison.rendement[0]
            rendement_final = df_saison.rendement[-1]
            decrement = abs(rendement_initial-rendement_final)/rendement_initial
            temp.append((debut_saison,fin_saison,rendement_initial,rendement_final,decrement,idx+1))
        self.df_train = pd.DataFrame(temp, columns=["debut_saison","fin_saison","rendement_initial","rendement_final","decrement","saison"])
        return self.df_train
