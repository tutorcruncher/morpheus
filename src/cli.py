import click
import logging
import requests
import uuid

from src.settings import Settings

logger = logging.getLogger('cli')
settings = Settings()


@click.group()
@click.pass_context
def cli(ctx):
    """
    Run morpheus CLI.
    """
    pass


@cli.command(name='send_email')
@click.argument('recipient')
@click.option('--html', default=None)
def send_email(recipient, html=None):
    """Send an email with an attached document."""
    data = dict(
        uid=str(uuid.uuid4()),
        main_template='<body>\nThis is an example message\n</body>',
        company_code='test-company',
        from_address='TutorCruncher <test@tutorcruncher.com>',
        method='email-mandrill',
        subject_template='test message',
        context={'message': 'this is a test'},
        recipients=[{'address': recipient, 'pdf_attachments': [{'name': 'test.pdf', 'html': html}] if html else []}],
    )
    r = requests.post(
        f'https://{settings.host_name}/send/email/', json=data, headers={'Authorization': settings.auth_key}
    )
    assert r.status_code == 201, r.content.decode()


if __name__ == '__main__':  # pragma: no cover
    cli()
