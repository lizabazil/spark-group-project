import pyspark.sql.functions as f

from columns import primary_profession, id_person


def top_10_professions_by_number_of_people(name_basics):
    """
    11. Top 10 most popular professions by number of people
    Args:
        name_basics: The name_basics dataframe.
    Returns:
        top_10_professions: dataframe with two columns: profession and people_number
    """
    profession = 'profession'
    people_number = 'people_number'
    top_10_professions = (name_basics
                          .withColumn(profession, f.explode(primary_profession))
                          .groupBy(profession)
                          .agg(f.countDistinct(id_person).alias(people_number))
                          .orderBy(people_number, ascending=False)
                          .limit(10))
    return top_10_professions
