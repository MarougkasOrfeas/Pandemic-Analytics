import webbrowser
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import ttk

from PIL import Image, ImageTk

from app.src.enums import AssetsPaths, ScreenProperties

"""
    Class for COVID-19 data analysis and visualization using MapReduce approach.
    @Desc: This approach involves two main steps: mapping and reducing.
    In the mapping step, the data is split into chunks and analyzed independently.
    In the reducing step, the analyzed data is combined to produce aggregated results.
    This class applies the MapReduce algorithm to extract various insights, including age counts,
    gender counts, patient type counts, and total deaths related to COVID-19 diseases.
    Once the reduction is completed, the class processes the aggregated data and generates interactive
    visualizations. These visualizations offer a comprehensive view of the data and facilitate
    deeper analysis of COVID-19.
"""


class Covid:

    def __init__(self):
        self.data = pd.read_csv(AssetsPaths.Paths.CSV_FILE.value)  # csv location
        self.num_files = 4  # Number of files that csv will be split

    """
    Split the data into chunks, apply mapping function to each chunk, shuffle and reduce the mapped data,
        and then process the reduced data.
    """
    def split_data(self):
        data_chunks = np.array_split(self.data, self.num_files)
        mapped_data = []
        for i, chunk in enumerate(data_chunks):
            # Call the map_data() method for the current chunk and append the result to the mapped_data list
            mapped_data.extend(self.map_data(chunk))

        # This shuffles the mapped data, reduces the shuffled data and process the reduced data
        # Note: process_data method calls the reduce_data method which calls the shuffle_data method
        # with the mapped_data array.
        self.process_data(self.reduce_data(self.shuffle_data(mapped_data)))

    """
    Map the data chunk to extract information about the needed graphs and add them on dictionary values .
        @param data_chunk: DataFrame chunk containing COVID-19 data
        @return: List of mapped data chunks
    """
    @staticmethod
    def map_data(data_chunk):
        """
        Graphs
        """
        # 1st
        age_counts = data_chunk['AGE']
        death_dates = data_chunk['DATE_DIED']
        # Replace '9999-99-99' with None for patients who are still alive
        death_dates = [date if date != '9999-99-99' else None for date in death_dates]
        # Count the number of deaths for each age
        death_for_each_age = [1 if date is not None else 0 for date in death_dates]

        # 2nd
        patient_mapping = {1: 'Not Hospitalized', 2: 'Hospitalized'}
        patient_type_counts = data_chunk['PATIENT_TYPE'].map(patient_mapping).value_counts()

        # 3rd
        hospitalization_columns = ['USMER', 'INTUBED', 'ICU']
        hospitalization_data = data_chunk[hospitalization_columns]
        data = hospitalization_data.astype(int)
        hospitalization_data = data[hospitalization_columns].sum()

        # 4th
        hospitalization_diseases_columns = ['DIABETES', 'COPD', 'ASTHMA', 'RENAL_CHRONIC', 'OBESITY',
                                            'OTHER_DISEASE', 'CARDIOVASCULAR', 'PNEUMONIA', 'INMSUPR', 'TOBACCO',
                                            'PATIENT_TYPE', 'DATE_DIED']
        hospitalization_diseases_data = data_chunk[hospitalization_diseases_columns]
        # Set 0 where 'DATE_DIED' is not equal to '9999-99-99' (ALIVE)
        hospitalization_diseases_data.loc[
            hospitalization_diseases_data['DATE_DIED'] != '9999-99-99', hospitalization_diseases_columns[:-2]] = 0
        # Set 0 where 'PATIENT_TYPE' is not equal to 2
        hospitalization_diseases_data.loc[
            hospitalization_diseases_data['PATIENT_TYPE'] != 2, hospitalization_diseases_columns[:-2]] = 0
        # Sum the diseases for each column except ('PATIENT_TYPE')
        # hospitalization_diseases_data = hospitalization_diseases_data.astype(int)
        hospitalization_diseases_data = hospitalization_diseases_data.loc[:, hospitalization_diseases_columns[:-2]].sum()

        # 5th
        diseases_columns = ['DIABETES', 'COPD', 'ASTHMA', 'RENAL_CHRONIC', 'OBESITY', 'OTHER_DISEASE', 'DATE_DIED']
        diseases_data = data_chunk[diseases_columns]
        diseases_data.loc[diseases_data['DATE_DIED'] == '9999-99-99', 'DATE_DIED'] = '0'
        diseases_data.loc[diseases_data['DATE_DIED'].str.match(r'\d{1,2}/\d{1,2}/\d{4}'), 'DATE_DIED'] = '1'
        data = diseases_data.astype(int)
        preconditions_diseases_deaths = data[diseases_columns[:-1]].sum()

        # 6th
        vulnerable_groups_columns = ['CARDIOVASCULAR', 'PNEUMONIA', 'OBESITY', 'INMSUPR', 'TOBACCO', 'DATE_DIED']
        vulnerable_data = data_chunk[vulnerable_groups_columns]
        vulnerable_data.loc[vulnerable_data['DATE_DIED'] == '9999-99-99', 'DATE_DIED'] = '0'
        vulnerable_data.loc[vulnerable_data['DATE_DIED'].str.match(r'\d{1,2}/\d{1,2}/\d{4}'), 'DATE_DIED'] = '1'
        vulnerable_data = vulnerable_data.astype(int)
        preconditions_vulverable_groups_deaths = vulnerable_data[vulnerable_groups_columns[:-1]].sum()

        # Create a dictionary with the mapped data
        mapped_chunk = {
            'age_counts': age_counts,
            'death_for_each_age': death_for_each_age,
            'patient_type_counts': patient_type_counts,
            'preconditions_diseases_deaths': preconditions_diseases_deaths,
            'preconditions_vulverable_groups_deaths': preconditions_vulverable_groups_deaths,
            'hospitalization_data': hospitalization_data,
            'hospitalization_diseases_data': hospitalization_diseases_data
        }

        return [mapped_chunk]

    """
    Shuffle and sort the mapped data by key.
        @param mapped_data: Mapped data chunks
        @return: Shuffled and sorted data
    """
    @staticmethod
    def shuffle_data(mapped_data):
        shuffled_data = []
        for chunk in mapped_data:
            if isinstance(chunk, dict):
                for key, value in chunk.items():
                    # Convert each key-value pair to a tuple and add to shuffled_data
                    shuffled_data.append((key, value))
            # Sort shuffled_data by the key
            shuffled_data.sort(key=lambda x: x[0])
            return shuffled_data

    """
    Reduce the shuffled data by combining values with the same key.
        @param shuffled_data: Shuffled and sorted data
        @return: Reduced data
    """
    @staticmethod
    def reduce_data(shuffled_data):
        reduced_data = {}
        for key, value in shuffled_data:
            if key not in reduced_data:
                # Add the key-value pair to the reduced_data dictionary
                reduced_data[key] = value
            else:
                # Add the value to the existing value for the key in reduced_data
                reduced_data[key] += value
        return reduced_data

    """
    Process and analyze the reduced data by generating visualizations.
        @param reduced_data: Reduced data
    """
    @staticmethod
    def process_data(reduced_data):
        # Process or analyze the reduced data
        patient_type_counts = reduced_data['patient_type_counts']
        preconditions_diseases_deaths = reduced_data['preconditions_diseases_deaths']
        preconditions_vulverable_groups_deaths = reduced_data['preconditions_vulverable_groups_deaths']
        hospitalization_data = reduced_data['hospitalization_data']
        age_counts = reduced_data['age_counts']
        death_for_each_age = reduced_data['death_for_each_age']
        hospitalization_diseases_data = reduced_data['hospitalization_diseases_data']

        """ 
        Create the main window  
        """
        window = tk.Tk()
        window.title('COVID-19 Data Analysis Graphs')
        window.geometry(ScreenProperties.Settings.SCREEN_DIMENSIONS.value)
        window.resizable(False, False)

        image = Image.open(AssetsPaths.Paths.BACKGROUND_IMAGE.value)
        image = image.resize(ScreenProperties.Settings.IMAGE_DIMENSIONS.value, Image.LANCZOS)
        background_image = ImageTk.PhotoImage(image)

        background_label = tk.Label(window, image=background_image)
        background_label.place(x=0, y=0, relwidth=1, relheight=1)

        """
        Create buttons for displaying the graphs
        """
        buttons = [
            {  # GRAPH 1
                'text': 'Covid Incidents/Deaths Based on Age',
                'command': lambda: Covid.display_age_data(age_counts, death_for_each_age),
                'position': (260, 100)
            },
            {  # GRAPH 2
                'text': 'Covid Incidents and Hospitalization',
                'command': lambda: Covid.display_patient_type_data(patient_type_counts),
                'position': (260, 150)
            },
            {  # GRAPH 3
                'text': 'Hospitalization types',
                'command': lambda: Covid.display_hospitalization_types(hospitalization_data),
                'position': (260, 200)
            },
            {  # GRAPH 4
                'text': 'Hospitalization percentage for patients medical history',
                'command': lambda: Covid.display_hospitalization_based_on_diseases(hospitalization_diseases_data),
                'position': (260, 250)
            },
            {  # GRAPH 5
                'text': 'COVID Deaths for each pre-condition vulnerabilities',
                'command': lambda: Covid.display_deaths_based_on_vulnerable_groups(preconditions_vulverable_groups_deaths),
                'position': (260, 300)
            },
            {  # GRAPH 6
                'text': 'COVID Deaths for each pre-condition diseases',
                'command': lambda: Covid.display_deaths_based_on_diseases(preconditions_diseases_deaths),
                'position': (260, 350)
            }
        ]

        for button in buttons:
            graph_button = ttk.Button(window, text=button['text'], command=button['command'])
            graph_button.config(width=ScreenProperties.Settings.BUTTON_WIDTH.value)
            graph_button.place(x=button['position'][0], y=button['position'][1])

        """
        Navigation to dataset online
        """
        dataset_link_label = tk.Label(window, text="Open the dataset online", fg="blue", cursor="hand2")
        dataset_link_label.place(x=650, y=560)
        dataset_link_label.bind("<Button-1>", lambda e: webbrowser.open(AssetsPaths.Paths.DATASET_ONLINE.value))

        # Start the main event loop
        window.mainloop()

    """
    GRAPH-1: Display a histogram showing COVID-19 cases and deaths by age.
        @param age_counts: List of age counts for COVID-19 cases.
        @param death_for_each_age: List indicating deaths for each age group.
    """
    @staticmethod
    def display_age_data(age_counts, death_for_each_age):
        plt.hist(age_counts, bins=20)
        plt.hist(age_counts, bins=20, weights=death_for_each_age, color='red', alpha=0.5)
        plt.xlabel('Age')
        plt.ylabel('Count')
        plt.title('COVID-19 Cases and Deaths by Age')
        plt.legend(['Cases', 'Deaths'])
        plt.xticks(range(min(age_counts), max(age_counts) + 1, 10))
        plt.show()

    """
    GRAPH-2: Display a pie chart showing the distribution of patient types (returned home vs. hospitalization).
        @param patient_type_counts: Patient type counts data
    """
    @staticmethod
    def display_patient_type_data(patient_type_counts):
        plt.pie(patient_type_counts.values, labels=patient_type_counts.index, autopct='%1.1f%%')
        plt.title('Patient Type Distribution')
        plt.legend(patient_type_counts.index, loc='best')
        plt.show()

    """
    GRAPH-3: Display a pie chart showing the percentage distribution of hospitalization types.
        @param hospitalization_data: Series containing the count of different hospitalization types.
    """
    @staticmethod
    def display_hospitalization_types(hospitalization_data):
        custom_labels = ['MEDICAL TREATMENT', 'INTUBED', 'INTENSIVE CARE']
        plt.figure(figsize=(10, 8))
        plt.pie(hospitalization_data.astype(int), labels=custom_labels, autopct='%1.1f%%')
        plt.title('Percentage of hospitalization types', loc='left', color='blue')
        plt.axis('equal')
        plt.tight_layout()
        plt.show()

    '''
    GRAPH-4: Display percentage of hospitalized patients based on diseases 
    @param hospitalization_diseases_data: Patient hospitalization diseases counts data
    '''
    @staticmethod
    def display_hospitalization_based_on_diseases(hospitalization_diseases_data):
        hospitalization_diseases_columns = ['DIABETES', 'COPD', 'ASTHMA', 'RENAL_CHRONIC', 'OBESITY',
                                            'OTHER_DISEASE','CARDIOVASCULAR', 'PNEUMONIA', 'INMSUPR', 'TOBACCO', 'PATIENT_TYPE', 'DATE_DIED']
        plt.figure(figsize=(10, 8))
        plt.pie(hospitalization_diseases_data.astype(int), labels=hospitalization_diseases_columns[:-2], autopct='%1.1f%%')
        plt.title('Hospitalization percentage for patients medical history', loc='left', color='blue')
        plt.axis('equal')
        plt.tight_layout()
        plt.show()

    """
    GRAPH-5: Display a pie chart showing the percentage of deaths for each group of preconditions.
        @param: vulverable_groups_deaths: Series containing the count of deaths for each vulnerable group.
    """
    @staticmethod
    def display_deaths_based_on_vulnerable_groups(vulverable_groups_deaths):
        vulnerable_groups_columns = ['CARDIOVASCULAR', 'PNEUMONIA', 'OBESITY', 'INMSUPR', 'TOBACCO', 'DATE_DIED']
        plt.figure(figsize=(10, 8))
        plt.pie(vulverable_groups_deaths.astype(int), labels=vulnerable_groups_columns[:-1], autopct='%1.1f%%')
        plt.title('Percentage of Deaths for each vulnerable group', loc='left', color='blue')
        plt.axis('equal')
        plt.tight_layout()
        plt.show()

    """
    GRAPH-6: Display a pie chart showing the percentage of deaths for each group of preconditions.
        @param: vulverable_groups_deaths: Series containing the count of deaths for each vulnerable group.
    """
    @staticmethod
    def display_deaths_based_on_diseases(diseases_deaths):
        diseases_columns = ['DIABETES', 'COPD', 'ASTHMA', 'RENAL_CHRONIC', 'OBESITY', 'OTHER_DISEASE', 'DATE_DIED']
        plt.figure(figsize=(10, 8))
        plt.pie(diseases_deaths.astype(int), labels=diseases_columns[:-1], autopct='%1.1f%%')
        plt.title('Percentage of Deaths for each disease', loc='left', color='blue')
        plt.axis('equal')
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    covid = Covid()
    covid.split_data()
