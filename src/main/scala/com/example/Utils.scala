package com.example

import org.apache.spark.sql.expressions.UserDefinedFunction

object Utils {

  val categories: Map[Int, String] = Map(
    1 -> "Film & Animation",
    2 -> "Autos & Vehicles",
    10 -> "Music",
    15 -> "Pets & Animals",
    17 -> "Sports",
    18 -> "Short Movies",
    19 -> "Travel & Events",
    20 -> "Gaming",
    21 -> "Videoblogging",
    22 -> "People & Blogs",
    23 -> "Comedy",
    24 -> "Entertainment",
    25 -> "News & Politics",
    26 -> "Howto & Style",
    27 -> "Education",
    28 -> "Science & Technology",
    29 -> "Nonprofits & Activism",
    30 -> "Movies",
    31 -> "Anime / Animation",
    32 -> "Action / Adventure",
    33 -> "Classics",
    34 -> "Comedy",
    35 -> "Documentary",
    36 -> "Drama",
    37 -> "Family",
    38 -> "Foreign",
    39 -> "Horror",
    40 -> "Sci - Fi / Fantasy",
    41 -> "Thriller",
    42 -> "Shorts",
    43 -> "Shows",
    44 -> "Trailers"
  )

  val months: Map[String, String] = Map(
    "01" -> "January",
    "02" -> "February",
    "03" -> "March",
    "04" -> "April",
    "05" -> "May",
    "06" -> "June",
    "07" -> "July",
    "08" -> "August",
    "09" -> "September",
    "10" -> "October",
    "11" -> "November",
    "12" -> "December"
  )

  import org.apache.spark.sql.functions.udf

  def extractCategory: UserDefinedFunction = {
    udf((key: Int) => categories.get(key))
  }

  def extractMonth: UserDefinedFunction = {
    udf((key: String) => months.get(key))
  }

  import java.time.Instant

  def getYear: UserDefinedFunction = udf((epochMillisString: String) => {
    epochMillisString match {
      case null => "n/a"
      case _ =>
        val epochMillisLong = epochMillisString.toLong
        val instantString = Instant.ofEpochMilli(epochMillisLong).toString
        val instantStrings = instantString.split("-")
        instantStrings(0)
    }
  })

  def getMonth: UserDefinedFunction = udf((epochMillisString: String) => {
    epochMillisString match {
      case null => "n/a"
      case _ =>
        val epochMillisLong = epochMillisString.toLong
        val instantString = Instant.ofEpochMilli(epochMillisLong).toString
        val instantStrings = instantString.split("-")
        instantStrings(1)
    }
  })

  def extractDuration: UserDefinedFunction = udf((durationString: String) => {
    if (durationString.contains("H") && durationString.contains("M") && durationString.contains("S")) {
      val durationH = durationString.replace("PT", "H").replace("M", "H").replace("S", "H")
      val durationStrings = durationH.split("H")
      val hours = durationStrings(1).toInt
      val minutes = durationStrings(2).toInt
      val seconds = durationStrings(3).toInt
      val durationSeconds = hours * 3600 + minutes * 60 + seconds
      durationSeconds
    }
    else if (durationString.contains("H") && !durationString.contains("M") && durationString.contains("S")) {
      val durationH = durationString.replace("PT", "H").replace("S", "H")
      val durationStrings = durationH.split("H")
      val hours = durationStrings(1).toInt
      val seconds = durationStrings(2).toInt
      val durationSeconds = hours * 3600 + seconds
      durationSeconds
    }
    else if (durationString.contains("H") && durationString.contains("M") && !durationString.contains("S")) {
      val durationH = durationString.replace("PT", "H").replace("M", "H")
      val durationStrings = durationH.split("H")
      val hours = durationStrings(1).toInt
      val minutes = durationStrings(2).toInt
      val durationSeconds = hours * 3600 + minutes * 60
      durationSeconds
    }
    else if (durationString.contains("H") && !durationString.contains("M") && !durationString.contains("S")) {
      val durationH = durationString.replace("PT", "H")
      val durationStrings = durationH.split("H")
      val hours = durationStrings(1).toInt
      val durationSeconds = hours * 3600
      durationSeconds
    }

    else if (!durationString.contains("H") && durationString.contains("M") && durationString.contains("S")) {
      // first, we replace all letters with 'M', so we get 'MxxMxM'
      val durationM = durationString.replace("PT", "M").replace("S", "M")
      // then we split by 'M'
      val durationStrings = durationM.split("M")
      // minutes:
      val minutes = durationStrings(1).toInt
      // seconds:
      val seconds = durationStrings(2).toInt
      val durationSeconds = minutes * 60 + seconds
      durationSeconds
    }
    else if (!durationString.contains("H") && durationString.contains("M") && !durationString.contains("S")) {
      // to get 'MxxM'
      val durationM = durationString.replace("PT", "M")
      val durationStrings = durationM.split("M")
      val minutes = durationStrings(1).toInt
      val durationSeconds = minutes * 60
      durationSeconds
    }
    else if (!durationString.contains("H") && !durationString.contains("M") && durationString.contains("S")) {
      // to get 'SxxS'
      val durationS = durationString.replace("PT", "S")
      val durationStrings = durationS.split("S")
      val seconds = durationStrings(1).toInt
      seconds
    }
    else {
      0
    }
  })

}
