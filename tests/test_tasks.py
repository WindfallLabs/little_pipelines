"""Tests for Task class functionality."""

import pytest
from pathlib import Path

import little_pipelines as lp
from .conftest import PIPELINE_NAME, DO_LOGGING


class TestTaskBasics:
    """Test basic Task initialization and properties."""
    
    def test_task_initialization(self):
        """Test Task can be initialized with required parameters."""
        task = lp.Task(name="TestTask")
        
        assert task.name == "TestTask"
        assert task._dependency_names == []
        assert task.is_executed == False
        assert task.is_skipped == False
        assert task.logger is None
        assert task.pipeline is None
        assert task._script_path == __file__
    
    def test_task_with_dependencies(self):
        """Test Task initialization with dependencies."""
        task = lp.Task(
            name="DependentTask",
            dependencies=["TaskA", "TaskB"]
        )
        
        assert task._dependency_names == ["TaskA", "TaskB"]
        assert task.dependencies is None  # No pipeline yet
    
    def test_task_with_input_files(self):
        """Test Task initialization with input files."""
        input_files = [Path("file1.txt"), "file2.csv"]
        task = lp.Task(
            name="FileTask",
            input_files=input_files
        )
        
        assert task.input_files == input_files
        assert task.hash_inputs == True
    
    def test_task_without_hash_inputs(self):
        """Test Task with hash_inputs disabled."""
        task = lp.Task(
            name="APITask",
            hash_inputs=False
        )
        
        assert task.hash_inputs == False
        assert task._inputs_hash == ""


class TestTaskPipelineIntegration:
    """Test Task integration with Pipeline."""
    
    def test_task_added_to_pipeline(self, clean_pipeline):
        """Test Task is properly configured when added to Pipeline."""
        pipeline = clean_pipeline
        task = lp.Task(name="TestTask")
        task._enable_logging = DO_LOGGING
        
        pipeline.add(task)
        
        assert task.pipeline == pipeline
        assert task.logger is not None
        assert task.log_dir is not None or not task._enable_logging
    
    def test_task_dependencies_resolved(self, clean_pipeline):
        """Test Task dependencies are resolved after adding to Pipeline."""
        pipeline = clean_pipeline
        task_a = lp.Task(name="TaskA")
        task_b = lp.Task(name="TaskB", dependencies=["TaskA"])
        task_a._enable_logging = DO_LOGGING
        task_b._enable_logging = DO_LOGGING
        
        pipeline.add(task_a, task_b)
        
        assert task_b.dependencies is not None
        assert "TaskA" in task_b.dependencies
        assert task_b.dependencies["TaskA"] == task_a
    
    def test_task_result_access(self, clean_pipeline):
        """Test Task result can be accessed via property."""
        pipeline = clean_pipeline
        task = lp.Task(name="ResultTask")
        task._enable_logging = DO_LOGGING
        
        @task.process
        def run(this):
            return "test_result"
        
        pipeline.add(task)
        assert task.result is None
        
        pipeline.execute()
        assert task.result == "test_result"


class TestTaskProcessDecorator:
    """Test the @task.process decorator functionality."""
    
    def test_process_decorator_basic(self, clean_pipeline):
        """Test basic process decorator usage."""
        pipeline = clean_pipeline
        task = lp.Task(name="DecoratorTask")
        task._enable_logging = DO_LOGGING
        
        @task.process
        def run(this):
            return 42
        
        pipeline.add(task)
        pipeline.execute()
        
        assert task.is_executed == True
        assert task.result == 42
    
    def test_process_decorator_multiple_methods(self, clean_pipeline):
        """Test multiple process methods on same task."""
        pipeline = clean_pipeline
        task = lp.Task(name="MultiMethodTask")
        task._enable_logging = DO_LOGGING
        
        @task.process
        def setup(this):
            return "setup_complete"
        
        @task.process
        def run(this):
            setup_status = this.setup()
            return f"ran_after_{setup_status}"
        
        pipeline.add(task)
        pipeline.execute()
        
        assert task.result == "ran_after_setup_complete"
        assert len(task._process_times) == 2
    
    def test_process_timing_recorded(self, clean_pipeline):
        """Test that process execution times are recorded."""
        pipeline = clean_pipeline
        task = lp.Task(name="TimingTask")
        task._enable_logging = DO_LOGGING
        
        @task.process
        def run(this):
            return "done"
        
        pipeline.add(task)
        pipeline.execute()
        
        assert len(task._process_times) > 0
        assert task._process_times[0][0] == "run"
        assert isinstance(task._process_times[0][1], str)


class TestTaskSkipping:
    """Test Task skipping functionality."""
    
    def test_task_skip_setter(self):
        """Test is_skipped can be set."""
        task = lp.Task(name="SkipTask")
        
        assert task.is_skipped == False
        task.is_skipped = True
        assert task.is_skipped == True
    
    def test_skipped_task_not_executed(self, clean_pipeline):
        """Test skipped tasks are not marked as executed."""
        pipeline = clean_pipeline
        task = lp.Task(name="SkipTask")
        task._enable_logging = DO_LOGGING
        
        @task.process
        def run(this):
            return "should_not_run"
        
        pipeline.add(task)
        pipeline.set_ignored("SkipTask")
        pipeline.execute()
        
        assert task.is_skipped == True
        assert task.is_executed == False


class TestTaskHashing:
    """Test Task hashing for cache invalidation."""
    
    def test_script_hash_generated(self, clean_pipeline):
        """Test that script hash is generated for tasks."""
        pipeline = clean_pipeline
        task = lp.Task(name="HashTask")
        task._enable_logging = DO_LOGGING
        
        @task.process
        def run(this):
            return "test"
        
        pipeline.add(task)
        
        script_hash = task._script_hash
        assert script_hash is not None
        assert isinstance(script_hash, str)
        assert len(script_hash) > 0
    
    def test_inputs_hash_empty_when_no_files(self, clean_pipeline):
        """Test inputs hash is empty when no input files specified."""
        pipeline = clean_pipeline
        task = lp.Task(name="NoInputsTask")
        task._enable_logging = DO_LOGGING
        
        pipeline.add(task)
        
        assert task._inputs_hash == ""
    
    def test_inputs_hash_disabled(self, clean_pipeline):
        """Test inputs hash is empty when hash_inputs=False."""
        pipeline = clean_pipeline
        task = lp.Task(
            name="NoHashTask",
            input_files=[Path("test.txt")],
            hash_inputs=False
        )
        task._enable_logging = DO_LOGGING
        
        pipeline.add(task)
        
        assert task._inputs_hash == ""


class TestTaskExecution:
    """Test task execution scenarios."""
    
    def test_execute_with_experimental(self):
        """Test experimental execute_with method."""
        task = lp.Task(name="SoloTask")
        task._enable_logging = DO_LOGGING
        
        @task.process
        def run(this):
            return 99
        
        task.execute_with(PIPELINE_NAME)
        
        assert task.is_executed == True
        assert task.result == 99
    
    def test_task_without_run_method_fails(self, clean_pipeline):
        """Test that task without run method raises error."""
        pipeline = clean_pipeline
        task = lp.Task(name="NoRunTask")
        task._enable_logging = DO_LOGGING
        
        pipeline.add(task)
        
        with pytest.raises(AttributeError, match="missing 'run' method"):
            pipeline.execute()


class TestTaskRepr:
    """Test Task string representation."""
    
    def test_task_repr(self):
        """Test Task __repr__ method."""
        task = lp.Task(name="ReprTask")
        
        assert repr(task) == "<Task ('ReprTask')>"
