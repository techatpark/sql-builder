package com.techatpark;

/**
 * Data Holder for the table - movie.
 *@param id Short.
 *@param title String.
 *@param directedBy String.
 */
public record Movie(
        Short id,

        String title,

        String directedBy
) {


    /**
     * gets value of column - theId.
     * @param theId
     * @return theId
     */
    public Movie withId(final Short theId) {
        return new Movie(
                theId,
                title,
                directedBy);
    }


    /**
     * gets value of column - theTitle.
     * @param theTitle
     * @return theTitle
     */
    public Movie withTitle(final String theTitle) {
        return new Movie(
                id,
                theTitle,
                directedBy);
    }


    /**
     * gets value of column - theDirectedBy.
     * @param theDirectedBy
     * @return theDirectedBy
     */
    public Movie withDirectedBy(final String theDirectedBy) {
        return new Movie(
                id,
                title,
                theDirectedBy);
    }

}




