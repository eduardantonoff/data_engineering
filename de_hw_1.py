import os
import io
import tarfile
import gzip
import shutil
import subprocess
import pandas as pd
import luigi

from luigi.util import requires
from datetime import datetime

# Задача для загрузки датасета
class DownloadDataset(luigi.Task):
    dataset_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"data/{self.dataset_name}/GSE68849_RAW.tar")
    
    def run(self):
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name}&format=file"
        subprocess.run(["curl", "-o", self.output().path, url], check=True)

# Задача для извлечения файлов из архива
class ExtractTarFile(luigi.Task):
    dataset_name = luigi.Parameter()

    def requires(self):
        return DownloadDataset(self.dataset_name)
    
    def output(self):
        return luigi.LocalTarget(f"data/{self.dataset_name}/extracted")
    
    def run(self):
        os.makedirs(self.output().path, exist_ok=True)
        with tarfile.open(self.input().path, "r") as tar:
            tar.extractall(path=self.output().path)

        folder_counter = 1
        for root, _, files in os.walk(self.output().path):
            for file in files:
                if file.endswith(".gz"):
                    gz_path = os.path.join(root, file)
                    folder_name = f"file_{folder_counter}"
                    folder_counter += 1
                    extract_folder = os.path.join(root, folder_name)
                    os.makedirs(extract_folder, exist_ok=True)
    
                    extracted_file_path = os.path.join(extract_folder, os.path.basename(gz_path[:-3]))
                    with gzip.open(gz_path, 'rb') as f_in:
                        with open(extracted_file_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)

                    os.remove(gz_path)

# Задача для обработки файлов
@requires(ExtractTarFile)
class ProcessFiles(luigi.Task):
    dataset_name = luigi.Parameter()

    def requires(self):
        return ExtractTarFile(self.dataset_name)
    
    def output(self):
        return luigi.LocalTarget(f"data/{self.dataset_name}/processed/timestamp.txt")
    
    def run(self):
        base_processed_path = f"data/{self.dataset_name}/processed"

        for root, dirs, files in os.walk(self.input().path):
            for file in files:
                if file.endswith(".txt"):
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(root, self.input().path)
                    processed_file_directory = os.path.join(base_processed_path, relative_path)
                    os.makedirs(processed_file_directory, exist_ok=True)
                    self.process_txt_file(file_path, processed_file_directory)

        with self.output().open('w') as f:
            f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    def process_txt_file(self, filepath, output_directory):
        section_data = {}
        current_section = None
        buffer = io.StringIO()

        with open(filepath, 'r') as file:
            for line in file:
                if line.startswith('['):
                    if current_section:
                        buffer = self.process_section(buffer, 
                                                      filepath, 
                                                      current_section, 
                                                      section_data, 
                                                      output_directory)
                        
                    current_section = line.strip('[]\n')
                else:
                    print(line, file=buffer)

            if current_section:
                buffer = self.process_section(buffer, 
                                              filepath, 
                                              current_section, 
                                              section_data, 
                                              output_directory)

        for section_name, df in section_data.items():
            full_output_file_path = os.path.join(output_directory, section_name)
            df.to_csv(full_output_file_path, index=False)
            
            if "Probes" in section_name:
                reduced_df = df.drop(columns=['Definition', 
                                              'Ontology_Component', 
                                              'Ontology_Process', 
                                              'Ontology_Function', 
                                              'Synonyms', 
                                              'Obsolete_Probe_Id', 
                                              'Probe_Sequence'])
                
                reduced_file_name = f"reduced_{section_name}"
                reduced_output_file_path = os.path.join(output_directory, reduced_file_name)
                reduced_df.to_csv(reduced_output_file_path, index=False)

    def process_section(self, buffer, filepath, current_section, section_data, output_directory):
        buffer.seek(0)
        df = pd.read_csv(buffer, sep='\t', header=0)
        section_name = f"{os.path.splitext(os.path.basename(filepath))[0]}_{current_section}.csv"
        section_data[section_name] = df
        return io.StringIO()

class Pipeline(luigi.WrapperTask):
    
    dataset_name = luigi.Parameter(default="GSE68849")

    def requires(self):
        return ProcessFiles(dataset_name=self.dataset_name)

if __name__ == "__main__":
    luigi.build([Pipeline()], local_scheduler=True)