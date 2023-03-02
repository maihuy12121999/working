namespace java course_test

enum PhoneType {
   HOME,
   WORK,
   MOBILE,
   OTHER
}

struct Phone{
    1: i32 id,
    2: string number,
    3:PhoneType type
}

struct Person {
    1: i32 id,
    2: string firstName,
    3: string lastName,
    4: string email,
    5: list<Phone> phones
}

struct Course {
    1: i32 id,
    2: string number,
    3: string name,
    4: Person instructor,
    5: string roomNumber,
    6: list<Person> students
}

exception CourseNotFound {
 1:string message = "Cannot found the course"
}

exception UnacceptableCourse {
    1:string message = "The course is unacceptable"
}

service CourseService {
    list<string> getCourseInventory(),
    Course getCourse(1:string courseNumber) throws (1:CourseNotFound cnf),
    void addCourse(1:Course course) throws (1: UnacceptableCourse uc),
    void deleteCourse(1:string courseNumber) throws (1:CourseNotFound cnf)
}
