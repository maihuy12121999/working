package course_test;

import course_test.course_service.*;
import org.apache.thrift.TException;

import java.util.*;

public class CourseHandler implements CourseService.Iface {
    List<Course> courses = new ArrayList<>();
    @Override
    public List<String> getCourseInventory() throws TException {
        return Arrays.asList("data structure","algorithm","basic coding","advance coding");
    }

    @Override
    public Course getCourse(String courseNumber) throws CourseNotFound, TException {
        return null;
    }

    @Override
    public void addCourse(Course course) throws UnacceptableCourse, TException {
        courses.add(course);
    }

    @Override
    public void deleteCourse(String courseNumber) throws CourseNotFound, TException {

    }

}