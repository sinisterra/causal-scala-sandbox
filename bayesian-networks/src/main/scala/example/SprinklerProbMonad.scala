import probability_monad._
import probability_monad.Distribution._

object SprinklerProbMonad {

  /**
    * digraph Sprinker {
    *  Season -> Rain;
    *  Rain -> Sprinkler;
    *  Rain -> Wet_Grass;
    *  Sprinkler -> Wet_Grass;
    * }
    *
    */
  def run() {
    sealed trait Season
    case object Spring extends Season
    case object Summer extends Season
    case object Autumn extends Season
    case object Winter extends Season

    val season = discreteUniform(List(Spring, Summer, Autumn, Winter))

    val winterProb: Double = 4 / 121
    val springProb: Double = 12 / 121
    val summerProb: Double = 36 / 121
    val autumnProb: Double = 6 / 121

    def rain(season: Season): Distribution[Boolean] = {
      season match {
        case Winter => tf(0.2)
        case Spring => tf(0.1)
        case Summer => tf(0.5)
        case Autumn => tf(0.3)
      }
    }

    def sprinkler(rain: Boolean): Distribution[Boolean] = {
      rain match {
        case true  => tf(0.01)
        case false => tf(0.6)
      }
    }

    def wetGrass(sprinkler: Boolean, rain: Boolean): Distribution[Boolean] = {
      (sprinkler, rain) match {
        case (false, false) => tf(0)
        case (false, true)  => tf(0.2)
        case (true, false)  => tf(0.9)
        case (true, true)   => tf(0.99)
      }
    }

    case class GrassHumidity(
      season: Season,
      sprinkler: Boolean,
      rain: Boolean,
      wetGrass: Boolean
    )

    val grassHumidity: Distribution[GrassHumidity] = for {
      s   <- season
      r   <- rain(s)
      spr <- sprinkler(r)
      wet <- wetGrass(spr, r)
    } yield GrassHumidity(s, r, spr, wet)

    println(
      grassHumidity.given(_.sprinkler).hist
    )
  }
}
