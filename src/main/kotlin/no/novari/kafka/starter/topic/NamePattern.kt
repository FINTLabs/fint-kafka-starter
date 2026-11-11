package no.novari.kafka.starter.topic

/**
 * Pattern for one part of a topic name, used by the pattern functions in [KafkaTopics]. Values are regex-quoted and
 * cannot contain `.` or uppercase letters.
 */
public class NamePattern private constructor(
    internal val regex: String,
) {
    public companion object {
        /** Any value. */
        public fun any(): NamePattern = NamePattern("[^.]+")

        /** One of [values]. Use one value for an exact match. */
        public fun anyOf(vararg values: String): NamePattern {
            require(values.isNotEmpty()) { "A topic name pattern needs at least one value." }
            values.forEach { validatePart("pattern value", it) }
            return NamePattern(values.joinToString("|", "(", ")") { it.replace(REGEX_SPECIAL_CHARACTERS, """\\$0""") })
        }

        private val REGEX_SPECIAL_CHARACTERS = Regex("""[\\^$.|?*+()\[\]{}]""")
    }
}
