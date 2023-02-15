from mrjob.job import MRJob

class Count(MRJob):

        def mapper(self, _, line):
                # for word in line.split():
                #         yield(word, 1)
                rating = line.split(",")[2]
                
                if not rating.isalpha():
                        yield(float(rating),1)
                # for temperature in list(line.split()[2:]):
                #         yield(temperature,1)

                #yield(tuple(line.split())[2:])
                        
        def reducer(self, word,counts):
                yield(word,sum(counts))

if __name__ == '__main__':
        Count.run()