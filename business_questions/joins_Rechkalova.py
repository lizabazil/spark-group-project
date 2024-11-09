import pyspark.sql.functions as f
from columns import region, directors, tconst, primary_name, title_id, id_person


def directors_with_projects_in_different_regions(title_akas, name_basics, title_crew):
    """
    27. Which directors have projects with the most translations (regions)?

    Args:
        title_akas (dataframe): The dataframe with titles with different translations.
        name_basics (dataframe): The dataframe with names of actors, directors etc.
        title_crew (dataframe): The dataframe with directors of films.

    Returns:
        dataframe: New dataframe with ids, names of directors with most projects in different regions and
        count of these regions.
    """
    # for columns in new dataframes
    director = "director"
    unique_regions_count = "unique_regions_count"

    title_akas = title_akas.select(title_id, region).filter(f.col(region) != "unknown")
    title_crew = title_crew.select(tconst, directors).filter(f.col(directors).isNotNull())

    title_crew_exploded = title_crew.withColumn(director, f.explode(f.col(directors))).select(tconst, director)

    directors_films_region = (title_crew_exploded
                              .join(title_akas, title_crew_exploded["tconst"] == title_akas["title_id"])
                              .select(director, region).distinct())

    directors_region_count = (directors_films_region
                              .groupBy(director)
                              .agg(f.countDistinct(region).alias(unique_regions_count)))

    directors_with_names = (directors_region_count
                            .join(name_basics, directors_region_count["director"] == name_basics["id_person"])
                            .select(id_person, primary_name, unique_regions_count)
                            .orderBy(f.col(unique_regions_count).desc()))

    return directors_with_names
